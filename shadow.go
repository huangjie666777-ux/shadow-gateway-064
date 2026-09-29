package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxComparisonBody = 1 << 20

type ShadowPolicy struct {
	Enabled       bool   `json:"enabled"`
	CandidateURL  string `json:"candidate_url"`
	PathPrefix    string `json:"path_prefix"`
	Concurrency   int    `json:"concurrency"`
	TimeoutMillis int    `json:"timeout_ms"`
}

type ShadowPolicySnapshot struct {
	Version int          `json:"version"`
	Policy  ShadowPolicy `json:"policy"`
}

type ShadowResult struct {
	ID              string `json:"id"`
	PolicyVersion   int    `json:"policy_version"`
	Path            string `json:"path"`
	PrimaryStatus   int    `json:"primary_status"`
	CandidateStatus int    `json:"candidate_status"`
	Verdict         string `json:"verdict"`
	Reason          string `json:"reason,omitempty"`
}

type ShadowStatus struct {
	Policy   ShadowPolicySnapshot `json:"policy"`
	Inflight int                  `json:"inflight"`
	Skipped  int64                `json:"skipped"`
	Results  []ShadowResult       `json:"results"`
}

type shadowVerification struct {
	id          string
	version     int
	path        string
	request     *http.Request
	done        chan capturedBody
	holder      *transportUse
	stopWatcher func()
}

type capturedBody struct {
	status int
	body   []byte
	reason string
}

type ShadowManager struct {
	mu         sync.Mutex
	version    int
	policy     ShadowPolicy
	inflight   int
	skipped    int64
	results    []ShadowResult
	closed     bool
	cancel     context.CancelFunc
	rootCtx    context.Context
	wg         sync.WaitGroup
	transports map[int]*transportUse
}

type transportUse struct {
	transport *http.Transport
	refs      int
}

func NewShadowManager() *ShadowManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ShadowManager{
		cancel:     cancel,
		rootCtx:    ctx,
		transports: make(map[int]*transportUse),
	}
}

func (m *ShadowManager) Publish(input ShadowPolicy) (ShadowPolicySnapshot, error) {
	if err := validateShadowPolicy(input); err != nil {
		return m.Snapshot(), err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return m.snapshotLocked(), errors.New("gateway is shutting down")
	}
	m.version++
	oldVersion := m.version - 1
	m.policy = input
	newEntry := &transportUse{transport: newShadowTransport(), refs: 1}
	oldEntry := m.transports[oldVersion]
	closeOld := false
	if oldEntry != nil {
		oldEntry.refs--
		closeOld = oldEntry.refs == 0
		if closeOld {
			delete(m.transports, oldVersion)
		}
	}
	m.transports[m.version] = newEntry
	snapshot := m.snapshotLocked()
	m.mu.Unlock()
	if closeOld {
		oldEntry.transport.CloseIdleConnections()
	}
	return snapshot, nil
}

func (m *ShadowManager) Snapshot() ShadowPolicySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *ShadowManager) snapshotLocked() ShadowPolicySnapshot {
	policy := m.policy
	return ShadowPolicySnapshot{Version: m.version, Policy: policy}
}

func (m *ShadowManager) Status() ShadowStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	results := make([]ShadowResult, len(m.results))
	copy(results, m.results)
	return ShadowStatus{
		Policy:   m.snapshotLocked(),
		Inflight: m.inflight,
		Skipped:  m.skipped,
		Results:  results,
	}
}

func (m *ShadowManager) Start(r *http.Request) *shadowVerification {
	if r.Method != http.MethodGet || r.ContentLength != 0 || len(r.TransferEncoding) > 0 {
		return nil
	}
	m.mu.Lock()
	if m.closed || !m.policy.Enabled || !strings.HasPrefix(r.URL.Path, m.policy.PathPrefix) || m.inflight >= m.policy.Concurrency {
		if !m.closed && m.policy.Enabled && strings.HasPrefix(r.URL.Path, m.policy.PathPrefix) && m.inflight >= m.policy.Concurrency {
			m.skipped++
		}
		m.mu.Unlock()
		return nil
	}
	version := m.version
	policy := m.policy
	entry := m.transports[version]
	if entry == nil {
		m.mu.Unlock()
		return nil
	}
	entry.refs++
	m.inflight++
	rootCtx := m.rootCtx
	m.mu.Unlock()

	candidate, err := newCandidateRequest(r, policy.CandidateURL)
	if err != nil {
		m.finish(version, entry, ShadowResult{}, false)
		return nil
	}

	id := newVerificationID()
	ctx, cancel := context.WithTimeout(rootCtx, time.Duration(policy.TimeoutMillis)*time.Millisecond)
	v := &shadowVerification{
		id:          id,
		version:     version,
		path:        r.URL.Path,
		request:     candidate,
		done:        make(chan capturedBody, 1),
		holder:      entry,
		stopWatcher: nil,
	}
	watcherDone := make(chan struct{})
	go func() {
		select {
		case <-r.Context().Done():
			cancel()
		case <-watcherDone:
		}
	}()
	v.stopWatcher = func() {
		close(watcherDone)
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer cancel()
		result := m.compare(ctx, v)
		m.finish(version, entry, result, true)
	}()
	return v
}

func (m *ShadowManager) compare(ctx context.Context, v *shadowVerification) ShadowResult {
	candidateDone := make(chan capturedBody, 1)
	go func() {
		candidateDone <- m.doCandidate(ctx, v)
	}()

	var primary, candidate capturedBody
	primaryDone := false
	candidateFinished := false
	for !(primaryDone && candidateFinished) {
		select {
		case primary = <-v.done:
			primaryDone = true
		case candidate = <-candidateDone:
			candidateFinished = true
		case <-ctx.Done():
			reason := "shadow timeout"
			if errors.Is(ctx.Err(), context.Canceled) {
				reason = "shadow canceled"
			}
			return m.result(v, 0, 0, "unknown", reason)
		}
	}
	if primary.reason != "" {
		return m.result(v, primary.status, candidate.status, "unknown", primary.reason)
	}
	if candidate.reason != "" {
		return m.result(v, primary.status, candidate.status, "unknown", candidate.reason)
	}
	if primary.status != candidate.status || !bytes.Equal(primary.body, candidate.body) {
		return m.result(v, primary.status, candidate.status, "different", "")
	}
	return m.result(v, primary.status, candidate.status, "equal", "")
}

func (m *ShadowManager) doCandidate(ctx context.Context, v *shadowVerification) capturedBody {
	resp, err := v.holder.transport.RoundTrip(v.request.WithContext(ctx))
	if err != nil {
		return capturedBody{reason: "candidate request failed: " + err.Error()}
	}
	defer resp.Body.Close()
	body, reason := readBoundedBody(resp.Body)
	if reason != "" {
		return capturedBody{status: resp.StatusCode, reason: reason}
	}
	return capturedBody{status: resp.StatusCode, body: body}
}

func (v *shadowVerification) completePrimary(primary capturedBody, clientGone bool) {
	if !clientGone {
		v.stopWatcher()
	}
	v.done <- primary
}

func (m *ShadowManager) finish(version int, entry *transportUse, result ShadowResult, record bool) {
	m.mu.Lock()
	m.inflight--
	shouldClose := false
	if record {
		m.results = append(m.results, result)
		if len(m.results) > 100 {
			m.results = m.results[len(m.results)-100:]
		}
	}
	entry.refs--
	activeVersion := version == m.activeVersionLocked()
	if entry.refs == 0 && !activeVersion {
		delete(m.transports, version)
		shouldClose = true
	} else if entry.refs < 0 {
		panic("shadow transport reference underflow")
	}
	m.mu.Unlock()
	if shouldClose {
		entry.transport.CloseIdleConnections()
	}
}

func (m *ShadowManager) activeVersionLocked() int {
	return m.version
}

func (m *ShadowManager) result(v *shadowVerification, primaryStatus, candidateStatus int, verdict, reason string) ShadowResult {
	return ShadowResult{
		ID:              v.id,
		PolicyVersion:   v.version,
		Path:            v.path,
		PrimaryStatus:   primaryStatus,
		CandidateStatus: candidateStatus,
		Verdict:         verdict,
		Reason:          reason,
	}
}

func (m *ShadowManager) Drain(ctx context.Context) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.mu.Unlock()

	stopCancelWatcher := context.AfterFunc(ctx, m.cancel)
	m.wg.Wait()
	stopCancelWatcher()
	m.mu.Lock()
	transports := make([]*http.Transport, 0, len(m.transports))
	for _, entry := range m.transports {
		transports = append(transports, entry.transport)
	}
	m.mu.Unlock()
	for _, transport := range transports {
		transport.CloseIdleConnections()
	}
}

func validateShadowPolicy(input ShadowPolicy) error {
	if input.Concurrency <= 0 {
		return errors.New("concurrency must be a positive integer")
	}
	if input.TimeoutMillis <= 0 {
		return errors.New("timeout_ms must be a positive integer")
	}
	if !strings.HasPrefix(input.PathPrefix, "/") {
		return errors.New("path_prefix must start with /")
	}
	return validateHTTPAddress(input.CandidateURL, "candidate_url")
}

func decodeShadowPolicy(r *http.Request) (ShadowPolicy, error) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	if err != nil {
		return ShadowPolicy{}, err
	}
	if len(payload) > 1<<20 {
		return ShadowPolicy{}, errors.New("request body exceeds 1 MiB")
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return ShadowPolicy{}, errors.New("request body must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	var policy ShadowPolicy
	if err := decoder.Decode(&policy); err != nil {
		return ShadowPolicy{}, err
	}
	return policy, nil
}

func newCandidateRequest(original *http.Request, candidateURL string) (*http.Request, error) {
	parsed, err := url.Parse(candidateURL)
	if err != nil {
		return nil, err
	}
	parsed.Path = original.URL.Path
	parsed.RawPath = original.URL.RawPath
	parsed.RawQuery = original.URL.RawQuery
	parsed.ForceQuery = original.URL.ForceQuery
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header = cloneOutboundHeaders(original.Header)
	request.Host = parsed.Host
	return request, nil
}

func cloneOutboundHeaders(input http.Header) http.Header {
	cloned := input.Clone()
	removeHopHeaders(cloned)
	return cloned
}

func newShadowTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = false
	return transport
}

func readBoundedBody(body io.Reader) ([]byte, string) {
	data, err := io.ReadAll(io.LimitReader(body, maxComparisonBody+1))
	if err != nil {
		return nil, "response read failed: " + err.Error()
	}
	if len(data) > maxComparisonBody {
		return nil, "response body exceeds 1 MiB"
	}
	return data, ""
}

func newVerificationID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw[:])
}
