package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProxyForwardsMethodPathQueryBodyAndResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusCreated)
		body, _ := io.ReadAll(r.Body)
		_, _ = fmt.Fprintf(w, "%s %s?%s host=%s body=%s", r.Method, r.URL.Path, r.URL.RawQuery, r.Host, body)
	}))
	defer upstream.Close()

	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "a", Address: upstream.URL, Limit: 1}})
	gateway := httptest.NewServer(NewGatewayHandler(pool, NewShadowManager()))
	defer gateway.Close()

	req, _ := http.NewRequest(http.MethodPost, gateway.URL+"/demo?x=1", strings.NewReader("payload"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated || string(body) != "POST /demo?x=1 host="+strings.TrimPrefix(upstream.URL, "http://")+" body=payload" {
		t.Fatalf("response = %d %q", resp.StatusCode, body)
	}
}

func TestNoCapacityReturns503(t *testing.T) {
	gateway := httptest.NewServer(NewGatewayHandler(NewPool(), NewShadowManager()))
	defer gateway.Close()
	resp, err := http.Post(gateway.URL, "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestConnectionFailureReturns502(t *testing.T) {
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	address := listener.URL
	listener.Close()

	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "dead", Address: address, Limit: 1}})
	gateway := httptest.NewServer(NewGatewayHandler(pool, NewShadowManager()))
	defer gateway.Close()

	resp, err := http.Get(gateway.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

func TestProxyRemovesConnectionSpecifiedHeaders(t *testing.T) {
	var gotHeader string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Gateway-Internal")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "a", Address: upstream.URL, Limit: 1}})
	gateway := httptest.NewServer(NewGatewayHandler(pool, NewShadowManager()))
	defer gateway.Close()

	req, _ := http.NewRequest(http.MethodGet, gateway.URL+"/", nil)
	req.Header.Set("X-Gateway-Internal", "secret")
	req.Header.Set("Connection", "X-Gateway-Internal")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotHeader != "" {
		t.Fatalf("Connection-specified header reached upstream: %q", gotHeader)
	}
}

func TestLeaseHeldUntilStreamEndAndClientCancelReleases(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("first\n"))
		flusher.Flush()
		close(started)
		<-r.Context().Done()
		close(finished)
	}))
	defer upstream.Close()

	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "a", Address: upstream.URL, Limit: 1}})
	gateway := httptest.NewServer(NewGatewayHandler(pool, NewShadowManager()))
	defer gateway.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(gateway.URL + "/slow")
	if err != nil {
		t.Fatal(err)
	}
	<-started
	waitForInflight(t, pool, "a", 1)

	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "first\n" {
		t.Fatalf("stream chunk = %q, %v", line, err)
	}
	waitForInflight(t, pool, "a", 1)

	_ = resp.Body.Close()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request was not canceled")
	}
	waitForInflight(t, pool, "a", 0)
}

func waitForInflight(t *testing.T, pool *Pool, id string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, status := range pool.Status() {
			if status.ID == id && status.Inflight == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("inflight for %s never became %d: %+v", id, want, pool.Status())
}
