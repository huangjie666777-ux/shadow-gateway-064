package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminReplaceConfigAndQueryStatus(t *testing.T) {
	pool := NewPool()
	router := NewAdminRouter(pool, NewShadowManager())
	server := httptest.NewServer(router)
	defer server.Close()

	body := []byte(`[{"id":"a","address":"http://127.0.0.1:9001","limit":2}]`)
	req, _ := http.NewRequest(http.MethodPut, server.URL+"/admin/upstreams", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("publish status = %d", resp.StatusCode)
	}

	resp, err = http.Get(server.URL + "/admin/config")
	if err != nil {
		t.Fatal(err)
	}
	var config ConfigSnapshot
	json.NewDecoder(resp.Body).Decode(&config)
	resp.Body.Close()
	if config.Version != 1 || config.Upstreams[0].ID != "a" {
		t.Fatalf("config = %+v", config)
	}

	resp, err = http.Get(server.URL + "/admin/nodes")
	if err != nil {
		t.Fatal(err)
	}
	var statuses []NodeStatus
	json.NewDecoder(resp.Body).Decode(&statuses)
	resp.Body.Close()
	if len(statuses) != 1 || statuses[0].State != "active" || statuses[0].Limit != 2 {
		t.Fatalf("statuses = %+v", statuses)
	}
}

func TestAdminRejectsInvalidWholeTable(t *testing.T) {
	pool := NewPool()
	_, _ = pool.Publish([]UpstreamConfig{{ID: "keep", Address: "http://127.0.0.1:9001", Limit: 1}})
	server := httptest.NewServer(NewAdminRouter(pool, NewShadowManager()))
	defer server.Close()

	body := []byte(`[{"id":"bad","address":"https://127.0.0.1:9002/path","limit":0}]`)
	req, _ := http.NewRequest(http.MethodPut, server.URL+"/admin/upstreams", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if got := pool.Config(); got.Version != 1 || got.Upstreams[0].ID != "keep" {
		t.Fatalf("config changed: %+v", got)
	}
}
