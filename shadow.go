package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const maxComparedBody = 1 << 20

const (
	shadowEqual     = "equal"
	shadowDifferent = "different"
	shadowUnknown   = "unknown"
)

type ShadowResult struct {
	ID              string `json:"id"`
	PolicyVersion   int    `json:"policy_version"`
	Path            string `json:"path"`
	PrimaryStatus   int    `json:"primary_status"`
	CandidateStatus int    `json:"candidate_status"`
	Decision        string `json:"decision"`
	Reason          string `json:"reason,omitempty"`
}

type ShadowStatus struct {
	Policy        ShadowPolicySnapshot `json:"policy"`
	Inflight      int                  `json:"inflight"`
	TotalSkipped  uint64               `json:"total_skipped"`
	RecentResults []ShadowResult       `json:"recent_results"`
}

type completedBody struct {
	status    int
	body      []byte
	oversized bool
	err       error
}

type shadowVerification struct {
	manager       *ShadowManager
	id            string
	version       int
	path          string
	candidateDone chan completedBody
	primaryDone   chan completedBody
	finalized     chan struct{}
	cancel        context.CancelFunc
	stopCancel    func() bool
}

type ShadowManager struct {
	mu         sync.Mutex
	policy     ShadowPolicy
	version    int
	closed     bool
	inflight   int
	skipped    uint64
	results    []ShadowResult
	wg         sync.WaitGroup
	nextID     atomic.Uint64
	rootCtx    context.Context
	rootCancel context.CancelFunc
	transport  *http.Transport
}

func NewShadowManager() *ShadowManager {
	ctx, cancel := context.WithCancel(context.Background())
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	return &ShadowManager{rootCtx: ctx, rootCancel: cancel, transport: transport, results: make([]ShadowResult, 0, 100)}
}

func (m *ShadowManager) Publish(input ShadowPolicy) (ShadowPolicySnapshot, error) {
	if err := validateShadowPolicy(input); err != nil {
		return m.Policy(), err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return m.policyLocked(), errors.New("gateway is shutting down")
	}
	m.version++
	m.policy = input
	return m.policyLocked(), nil
}

func (m *ShadowManager) Policy() ShadowPolicySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.policyLocked()
}

func (m *ShadowManager) policyLocked() ShadowPolicySnapshot {
	return ShadowPolicySnapshot{ShadowPolicy: m.policy, Version: m.version}
}

func (m *ShadowManager) Status() ShadowStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	results := make([]ShadowResult, len(m.results))
	copy(results, m.results)
	return ShadowStatus{Policy: m.policyLocked(), Inflight: m.inflight, TotalSkipped: m.skipped, RecentResults: results}
}

func (m *ShadowManager) Start(r *http.Request) (*shadowVerification, *cappedResponseWriter, bool) {
	if r.Method != http.MethodGet || r.ContentLength != 0 || len(r.TransferEncoding) > 0 {
		return nil, nil, false
	}
	m.mu.Lock()
	pathMatched := strings.HasPrefix(r.URL.Path, m.policy.PathPrefix)
	if m.closed || !m.policy.Enabled || !pathMatched {
		m.mu.Unlock()
		return nil, nil, false
	}
	if m.inflight >= m.policy.MaxConcurrency {
		m.skipped++
		m.mu.Unlock()
		return nil, nil, false
	}
	m.inflight++
	policy := m.policy
	version := m.version
	m.mu.Unlock()

	m.wg.Add(1)
	id := m.newID()
	requestCtx, cancel := context.WithCancel(m.rootCtx)
	stopCancel := context.AfterFunc(r.Context(), func() {
		if errors.Is(r.Context().Err(), context.Canceled) {
			cancel()
		}
	})
	verification := &shadowVerification{
		manager: m,
		id:      id, version: version, path: r.URL.Path,
		candidateDone: make(chan completedBody, 1), primaryDone: make(chan completedBody, 1),
		finalized: make(chan struct{}), cancel: cancel, stopCancel: stopCancel,
	}
	candidateReq := r.Clone(requestCtx)
	candidateReq.Body = nil
	candidateReq.GetBody = nil
	candidateReq.ContentLength = 0
	writer := &cappedResponseWriter{ResponseWriter: nil, verification: verification}

	go func(verification *shadowVerification, policy ShadowPolicy, candidateReq *http.Request) {
		candidate := m.fetchCandidate(candidateReq, policy)
		verification.candidateDone <- candidate
		select {
		case primary := <-verification.primaryDone:
			cancel()
			close(verification.finalized)
			m.record(compareShadow(verification, primary, candidate))
		case <-verification.finalized:
		}
	}(verification, policy, candidateReq)
	return verification, writer, true
}

func (m *ShadowManager) fetchCandidate(req *http.Request, policy ShadowPolicy) completedBody {
	candidateURL, err := url.Parse(policy.CandidateAddress)
	if err != nil {
		return completedBody{err: err}
	}
	req.URL.Scheme = candidateURL.Scheme
	req.URL.Host = candidateURL.Host
	req.Host = candidateURL.Host
	req.RequestURI = ""
	removeHopHeaders(req.Header)
	req.Close = true
	req.Header.Set("Connection", "close")

	client := &http.Client{Timeout: time.Duration(policy.TimeoutMillis) * time.Millisecond, Transport: m.transport}
	resp, err := client.Do(req)
	if err != nil {
		return completedBody{err: err}
	}
	defer resp.Body.Close()
	body, oversized, err := readCapped(resp.Body)
	return completedBody{status: resp.StatusCode, body: body, oversized: oversized, err: err}
}

func (v *shadowVerification) finishPrimary(w *cappedResponseWriter, err error) {
	v.stopCancel()
	v.primaryDone <- completedBody{status: w.status, body: append([]byte(nil), w.body...), oversized: w.oversized, err: err}
}

func (m *ShadowManager) record(result ShadowResult) {
	m.mu.Lock()
	m.results = append(m.results, result)
	if len(m.results) > 100 {
		m.results = m.results[len(m.results)-100:]
	}
	m.inflight--
	m.mu.Unlock()
	m.wg.Done()
}

func (m *ShadowManager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	stop := context.AfterFunc(ctx, m.rootCancel)
	defer stop()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	<-done
	m.rootCancel()
	m.transport.CloseIdleConnections()
}

func (m *ShadowManager) newID() string {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Sprintf("%d", m.nextID.Add(1))
	}
	return hex.EncodeToString(random[:])
}

type cappedResponseWriter struct {
	http.ResponseWriter
	verification *shadowVerification
	status       int
	writeErr     error
	body         []byte
	oversized    bool
}

func (w *cappedResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *cappedResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if !w.oversized && len(w.body) < maxComparedBody {
		remaining := maxComparedBody - len(w.body)
		if len(data) > remaining {
			w.body = append(w.body, data[:remaining]...)
			w.oversized = true
		} else {
			w.body = append(w.body, data...)
		}
	}
	n, err := w.ResponseWriter.Write(data)
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	return n, err
}

func (w *cappedResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *cappedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func compareShadow(v *shadowVerification, primary, candidate completedBody) ShadowResult {
	result := ShadowResult{ID: v.id, PolicyVersion: v.version, Path: v.path, PrimaryStatus: primary.status, CandidateStatus: candidate.status}
	if primary.err != nil {
		result.Decision = shadowUnknown
		result.Reason = "primary response interrupted: " + primary.err.Error()
		return result
	}
	if primary.oversized {
		result.Decision = shadowUnknown
		result.Reason = "primary body exceeds 1 MiB"
		return result
	}
	if candidate.err != nil {
		result.Decision = shadowUnknown
		result.Reason = candidateFailureReason(candidate.err)
		return result
	}
	if candidate.oversized {
		result.Decision = shadowUnknown
		result.Reason = "candidate body exceeds 1 MiB"
		return result
	}
	if primary.status != candidate.status {
		result.Decision = shadowDifferent
		result.Reason = "status code differs"
		return result
	}
	if !bytes.Equal(primary.body, candidate.body) {
		result.Decision = shadowDifferent
		result.Reason = "response body differs"
		return result
	}
	result.Decision = shadowEqual
	return result
}

func candidateFailureReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "candidate timeout"
	case errors.Is(err, context.Canceled):
		return "candidate canceled"
	default:
		return "candidate request failed: " + err.Error()
	}
}

func readCapped(reader io.Reader) ([]byte, bool, error) {
	limited := io.LimitReader(reader, maxComparedBody+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return body, len(body) > maxComparedBody, err
	}
	if len(body) > maxComparedBody {
		return body[:maxComparedBody], true, nil
	}
	return body, false, nil
}

func decodeShadowPolicy(r *http.Request) (ShadowPolicy, error) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	if err != nil {
		return ShadowPolicy{}, err
	}
	if len(payload) > 1<<20 {
		return ShadowPolicy{}, errors.New("request body exceeds 1 MiB")
	}
	var policy ShadowPolicy
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return ShadowPolicy{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ShadowPolicy{}, errors.New("request body must be a single JSON object")
	}
	return policy, nil
}

func removeHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			token = textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(token))
			if token != "" {
				header.Del(token)
			}
		}
	}
	for _, name := range hopHeaders {
		header.Del(name)
	}
}
