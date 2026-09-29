package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func publishTestShadow(t *testing.T, shadows *ShadowManager, candidate string, prefix string, concurrency, timeout int) {
	t.Helper()
	_, err := shadows.Publish(ShadowPolicy{
		Enabled:          true,
		CandidateAddress: candidate,
		PathPrefix:       prefix,
		MaxConcurrency:   concurrency,
		TimeoutMillis:    timeout,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitForShadowResult(t *testing.T, shadows *ShadowManager, decision string) ShadowResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status := shadows.Status()
		if len(status.RecentResults) > 0 {
			result := status.RecentResults[len(status.RecentResults)-1]
			if result.Decision == decision {
				return result
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("shadow result never became %s: %+v", decision, shadows.Status())
	return ShadowResult{}
}

func TestShadowComparesMatchingGet(t *testing.T) {
	var gotPath, gotQuery, gotHost, gotHeader string
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotHost, gotHeader = r.URL.Path, r.URL.RawQuery, r.Host, r.Header.Get("X-Shadow")
		w.Header().Set("X-Different", "ignored")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "same")
	}))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "same")
	}))
	defer primary.Close()

	pool, shadows := NewPool(), NewShadowManager()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 4}})
	publishTestShadow(t, shadows, candidate.URL, "/shadow", 2, 1000)
	gateway := httptest.NewServer(NewGatewayHandler(pool, shadows))
	defer gateway.Close()

	req, _ := http.NewRequest(http.MethodGet, gateway.URL+"/shadow/item?q=1", nil)
	req.Header.Set("X-Shadow", "kept")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	result := waitForShadowResult(t, shadows, shadowEqual)
	if result.PrimaryStatus != 202 || result.CandidateStatus != 202 || result.PolicyVersion != 1 || result.Path != "/shadow/item" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if gotPath != "/shadow/item" || gotQuery != "q=1" || gotHost != strings.TrimPrefix(candidate.URL, "http://") || gotHeader != "kept" {
		t.Fatalf("candidate request = path %q query %q host %q header %q", gotPath, gotQuery, gotHost, gotHeader)
	}
}

func TestShadowDetectsStatusAndBodyDifferenceAndLimitsResults(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "candidate") }))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "primary") }))
	defer primary.Close()

	pool, shadows := NewPool(), NewShadowManager()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 4}})
	publishTestShadow(t, shadows, candidate.URL, "/", 2, 1000)
	gateway := httptest.NewServer(NewGatewayHandler(pool, shadows))
	defer gateway.Close()

	resp, err := http.Get(gateway.URL + "/different")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	result := waitForShadowResult(t, shadows, shadowDifferent)
	if result.Reason != "response body differs" {
		t.Fatalf("reason = %q", result.Reason)
	}
}

func TestShadowRejectsPolicyAtomically(t *testing.T) {
	shadows := NewShadowManager()
	publishTestShadow(t, shadows, "http://127.0.0.1:9001", "/ok", 1, 100)
	_, err := shadows.Publish(ShadowPolicy{Enabled: true, CandidateAddress: "https://127.0.0.1:9002", PathPrefix: "bad", MaxConcurrency: 0, TimeoutMillis: 0})
	if err == nil {
		t.Fatal("expected validation error")
	}
	snapshot := shadows.Policy()
	if snapshot.Version != 1 || snapshot.PathPrefix != "/ok" || snapshot.MaxConcurrency != 1 {
		t.Fatalf("active policy changed: %+v", snapshot)
	}
}

func TestShadowOnlyMatchingGetWithoutBodyAndSkipsFullCapacity(t *testing.T) {
	release := make(chan struct{})
	candidateHits := make(chan string, 4)
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		candidateHits <- r.URL.Path
		<-release
		w.WriteHeader(200)
	}))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer primary.Close()

	pool, shadows := NewPool(), NewShadowManager()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 8}})
	publishTestShadow(t, shadows, candidate.URL, "/match", 1, 1000)
	gateway := httptest.NewServer(NewGatewayHandler(pool, shadows))
	defer gateway.Close()

	first, _ := http.Get(gateway.URL + "/match/one")
	<-candidateHits
	second, _ := http.Get(gateway.URL + "/match/two")
	_, _ = io.Copy(io.Discard, second.Body)
	second.Body.Close()
	thirdReq, _ := http.NewRequest(http.MethodPost, gateway.URL+"/match/three", strings.NewReader("body"))
	third, _ := http.DefaultClient.Do(thirdReq)
	_, _ = io.Copy(io.Discard, third.Body)
	third.Body.Close()
	fouth, _ := http.Get(gateway.URL + "/other")
	_, _ = io.Copy(io.Discard, fouth.Body)
	fouth.Body.Close()

	status := shadows.Status()
	if status.Inflight != 1 || status.TotalSkipped != 1 {
		t.Fatalf("status = inflight %d skipped %d", status.Inflight, status.TotalSkipped)
	}
	close(release)
	_, _ = io.Copy(io.Discard, first.Body)
	first.Body.Close()
	waitForShadowResult(t, shadows, shadowEqual)
	select {
	case hit := <-candidateHits:
		t.Fatalf("unexpected candidate hit %q", hit)
	default:
	}
}

func TestShadowOversizedBodyIsUnknownAndDoesNotSaveBody(t *testing.T) {
	large := bytes.Repeat([]byte("a"), maxComparedBody+1)
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(large) }))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(large) }))
	defer primary.Close()

	pool, shadows := NewPool(), NewShadowManager()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 4}})
	publishTestShadow(t, shadows, candidate.URL, "/", 2, 1000)
	gateway := httptest.NewServer(NewGatewayHandler(pool, shadows))
	defer gateway.Close()
	resp, _ := http.Get(gateway.URL + "/large")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	result := waitForShadowResult(t, shadows, shadowUnknown)
	if result.Reason != "candidate body exceeds 1 MiB" {
		t.Fatalf("reason = %q", result.Reason)
	}
}

func TestShadowTimeoutIsUnknown(t *testing.T) {
	release := make(chan struct{})
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { <-release; w.WriteHeader(200) }))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer primary.Close()

	pool, shadows := NewPool(), NewShadowManager()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 4}})
	publishTestShadow(t, shadows, candidate.URL, "/", 1, 50)
	gateway := httptest.NewServer(NewGatewayHandler(pool, shadows))
	defer gateway.Close()
	resp, _ := http.Get(gateway.URL + "/slow")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	result := waitForShadowResult(t, shadows, shadowUnknown)
	if result.Reason != "candidate timeout" {
		t.Fatalf("reason = %q", result.Reason)
	}
	close(release)
}

func TestConnectionHeadersAndTokensAreRemoved(t *testing.T) {
	received := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		w.WriteHeader(204)
	}))
	defer upstream.Close()
	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: upstream.URL, Limit: 1}})
	gateway := httptest.NewServer(NewGatewayHandler(pool, NewShadowManager()))
	defer gateway.Close()

	req, _ := http.NewRequest(http.MethodGet, gateway.URL, nil)
	req.Header.Set("Connection", "close, X-Token-Header")
	req.Header.Set("X-Token-Header", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	header := <-received
	if header.Get("Connection") != "" || header.Get("X-Token-Header") != "" {
		t.Fatalf("hop headers retained: %#v", header)
	}
}

func TestAdminShadowEndpoints(t *testing.T) {
	shadows := NewShadowManager()
	server := httptest.NewServer(NewAdminRouter(NewPool(), shadows))
	defer server.Close()
	body := fmt.Sprintf(`{"enabled":true,"candidate_address":"http://127.0.0.1:9002","path_prefix":"/v1","max_concurrency":2,"timeout_ms":250}`)
	req, _ := http.NewRequest(http.MethodPut, server.URL+"/admin/shadow", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var policy ShadowPolicySnapshot
	json.NewDecoder(resp.Body).Decode(&policy)
	resp.Body.Close()
	if policy.Version != 1 || !policy.Enabled || policy.MaxConcurrency != 2 {
		t.Fatalf("policy = %+v", policy)
	}

	resp, err = http.Get(server.URL + "/admin/shadow/status")
	if err != nil {
		t.Fatal(err)
	}
	var status ShadowStatus
	json.NewDecoder(resp.Body).Decode(&status)
	resp.Body.Close()
	if status.Inflight != 0 || status.TotalSkipped != 0 || len(status.RecentResults) != 0 {
		t.Fatalf("status = %+v", status)
	}
}
