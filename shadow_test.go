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

func TestClientDisconnectCancelsShadow(t *testing.T) {
	candidateStarted := make(chan struct{})
	candidateCanceled := make(chan struct{})
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(candidateStarted)
		<-r.Context().Done()
		close(candidateCanceled)
	}))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer primary.Close()

	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 1}})
	manager := NewShadowManager()
	_, _ = manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/", Concurrency: 1, TimeoutMillis: 5000})
	gateway := httptest.NewServer(NewGatewayHandler(pool, manager))
	defer gateway.Close()

	client := &http.Client{}
	req, _ := http.NewRequest(http.MethodGet, gateway.URL+"/slow-client", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	<-candidateStarted
	_ = resp.Body.Close()
	select {
	case <-candidateCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("candidate was not canceled after client disconnect")
	}
}

func TestShadowResultRingEvictsOldest(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer primary.Close()
	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 1}})
	manager := NewShadowManager()
	_, _ = manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/", Concurrency: 20, TimeoutMillis: 1000})
	gateway := httptest.NewServer(NewGatewayHandler(pool, manager))
	defer gateway.Close()

	for i := 0; i < 105; i++ {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%d", gateway.URL, i), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	deadline := time.Now().Add(3 * time.Second)
	var status ShadowStatus
	for time.Now().Before(deadline) {
		status = manager.Status()
		if len(status.Results) >= 100 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	status = manager.Status()
	if len(status.Results) != 100 || status.Results[0].Path != "/5" || status.Results[99].Path != "/104" {
		t.Fatalf("results len=%d first=%s last=%s", len(status.Results), status.Results[0].Path, status.Results[len(status.Results)-1].Path)
	}
}

func TestShadowPolicyAtomicPublishAndStatus(t *testing.T) {
	manager := NewShadowManager()
	valid := ShadowPolicy{Enabled: true, CandidateURL: "http://127.0.0.1:9002", PathPrefix: "/api", Concurrency: 2, TimeoutMillis: 500}
	snapshot, err := manager.Publish(valid)
	if err != nil || snapshot.Version != 1 {
		t.Fatalf("publish = %+v, %v", snapshot, err)
	}

	invalid := valid
	invalid.Concurrency = 0
	if _, err := manager.Publish(invalid); err == nil {
		t.Fatal("expected invalid policy to fail")
	}
	status := manager.Status()
	if status.Policy.Version != 1 || !status.Policy.Policy.Enabled || status.Policy.Policy.Concurrency != 2 {
		t.Fatalf("active policy changed: %+v", status.Policy)
	}
}

func TestShadowVerificationEndToEnd(t *testing.T) {
	tests := []struct {
		name      string
		primary   string
		candidate string
		verdict   string
	}{
		{name: "equal", primary: "same", candidate: "same", verdict: "equal"},
		{name: "different", primary: "one", candidate: "two", verdict: "different"},
		{name: "oversized", primary: "same", candidate: strings.Repeat("x", maxComparisonBody+1), verdict: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidateHost := ""
			candidatePath := ""
			candidateQuery := ""
			candidateSecret := ""
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Test", r.Header.Get("X-Test"))
				_, _ = io.WriteString(w, tc.primary)
			}))
			defer primary.Close()
			candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				candidateHost = r.Host
				candidatePath = r.URL.Path
				candidateQuery = r.URL.RawQuery
				candidateSecret = r.Header.Get("X-Secret")
				_, _ = io.WriteString(w, tc.candidate)
			}))
			defer candidate.Close()

			pool := NewPool()
			_, _ = pool.Publish([]UpstreamConfig{{ID: "primary", Address: primary.URL, Limit: 2}})
			manager := NewShadowManager()
			_, _ = manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/api", Concurrency: 1, TimeoutMillis: 1000})
			gateway := httptest.NewServer(NewGatewayHandler(pool, manager))
			defer gateway.Close()

			req, _ := http.NewRequest(http.MethodGet, gateway.URL+"/api/demo?x=1", nil)
			req.Header.Set("X-Test", "yes")
			req.Header.Set("X-Secret", "shadow-token")
			req.Header.Add("Connection", "X-Secret")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if string(body) != tc.primary || resp.Header.Get("X-Test") != "yes" {
				t.Fatalf("primary response changed: %q header=%q", body, resp.Header.Get("X-Test"))
			}
			if candidateHost != strings.TrimPrefix(candidate.URL, "http://") || candidatePath != "/api/demo" || candidateQuery != "x=1" || candidateSecret != "" {
				t.Fatalf("candidate request = path=%q query=%q host=%q secret=%q", candidatePath, candidateQuery, candidateHost, candidateSecret)
			}

			result := waitForShadowResult(t, manager)
			if result.Verdict != tc.verdict || result.PolicyVersion != 1 || result.Path != "/api/demo" || result.PrimaryStatus != 200 || result.CandidateStatus != 200 {
				t.Fatalf("result = %+v, want verdict %s", result, tc.verdict)
			}
		})
	}
}

func TestShadowSkipsWhenAtCapacityAndOnlyGetWithoutBody(t *testing.T) {
	hold := make(chan struct{}, 1)
	releaseCandidate := make(chan struct{})
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("candidate received method %s", r.Method)
		}
		select {
		case hold <- struct{}{}:
		default:
		}
		<-releaseCandidate
		_, _ = io.WriteString(w, "ok")
	}))
	defer candidate.Close()

	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer primary.Close()
	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 4}})
	manager := NewShadowManager()
	_, _ = manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/", Concurrency: 1, TimeoutMillis: 5000})
	gateway := httptest.NewServer(NewGatewayHandler(pool, manager))
	defer gateway.Close()

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		resp, err := http.Get(gateway.URL + "/one")
		if err != nil {
			return
		}
		<-hold
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	waitInflightShadow(t, manager, 1)
	if resp, err := http.Get(gateway.URL + "/two"); err != nil {
		t.Fatal(err)
	} else {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	status := manager.Status()
	if status.Inflight != 1 || status.Skipped != 1 {
		t.Fatalf("status = %+v, want one inflight and one skipped", status)
	}
	resp, err := http.Post(gateway.URL+"/three", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	status = manager.Status()
	if status.Skipped != 1 {
		t.Fatalf("POST was shadowed: %+v", status)
	}
	close(releaseCandidate)
	<-firstDone
}

func TestShadowTimeoutIsUnknownAndDoesNotDelayPrimary(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer primary.Close()

	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 1}})
	manager := NewShadowManager()
	_, _ = manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/", Concurrency: 1, TimeoutMillis: 20})
	gateway := httptest.NewServer(NewGatewayHandler(pool, manager))
	defer gateway.Close()

	start := time.Now()
	resp, err := http.Get(gateway.URL)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("primary waited for shadow for %s", elapsed)
	}
	result := waitForShadowResult(t, manager)
	if result.Verdict != "unknown" || !strings.Contains(result.Reason, "timeout") {
		t.Fatalf("result = %+v", result)
	}
}

func TestShadowVersionFixedAndBudgetSharedAcrossVersions(t *testing.T) {
	holdCandidates := make(chan struct{})
	versions := make(chan int, 4)
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1" {
			versions <- 1
		} else {
			versions <- 2
		}
		<-holdCandidates
		_, _ = io.WriteString(w, "ok")
	}))
	defer candidate.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer primary.Close()
	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "p", Address: primary.URL, Limit: 4}})
	manager := NewShadowManager()
	_, _ = manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/", Concurrency: 1, TimeoutMillis: 5000})
	gateway := httptest.NewServer(NewGatewayHandler(pool, manager))
	defer gateway.Close()

	resp, err := http.Get(gateway.URL + "/v1")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	<-versions
	waitInflightShadow(t, manager, 1)

	_, err = manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/", Concurrency: 0, TimeoutMillis: 5000})
	if err == nil {
		t.Fatal("invalid policy unexpectedly accepted")
	}
	snapshot, err := manager.Publish(ShadowPolicy{Enabled: true, CandidateURL: candidate.URL, PathPrefix: "/", Concurrency: 1, TimeoutMillis: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 2 {
		t.Fatalf("version = %d, want 2", snapshot.Version)
	}
	waitInflightShadow(t, manager, 1)

	resp, err = http.Get(gateway.URL + "/v2")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if status := manager.Status(); status.Skipped != 1 || status.Inflight != 1 {
		t.Fatalf("status = %+v, want old task to share budget and new request skipped", status)
	}
	close(holdCandidates)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status := manager.Status()
		if status.Inflight == 0 && len(status.Results) == 1 && status.Results[0].PolicyVersion == 1 && status.Policy.Version == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("old task did not finish: %+v", manager.Status())
}

func TestAdminShadowEndpoints(t *testing.T) {
	router := NewAdminRouter(NewPool(), NewShadowManager())
	server := httptest.NewServer(router)
	defer server.Close()
	body := bytes.NewBufferString(`{"enabled":true,"candidate_url":"http://127.0.0.1:9002","path_prefix":"/api","concurrency":3,"timeout_ms":250}`)
	req, _ := http.NewRequest(http.MethodPut, server.URL+"/admin/shadow-policy", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	resp, err = http.Get(server.URL + "/admin/shadow-status")
	if err != nil {
		t.Fatal(err)
	}
	var status ShadowStatus
	json.NewDecoder(resp.Body).Decode(&status)
	resp.Body.Close()
	if status.Policy.Version != 1 || status.Inflight != 0 || status.Results == nil {
		t.Fatalf("status = %+v", status)
	}
}

func waitForShadowResult(t *testing.T, manager *ShadowManager) ShadowResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status := manager.Status()
		if len(status.Results) > 0 {
			return status.Results[len(status.Results)-1]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("shadow result was not recorded")
	return ShadowResult{}
}

func waitInflightShadow(t *testing.T, manager *ShadowManager, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if manager.Status().Inflight == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("shadow inflight never became %d: %+v", want, manager.Status())
}
