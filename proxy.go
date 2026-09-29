package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
)

type GatewayHandler struct {
	pool    *Pool
	shadows *ShadowManager
}

func NewGatewayHandler(pool *Pool, shadows *ShadowManager) http.Handler {
	return &GatewayHandler{pool: pool, shadows: shadows}
}

func (h *GatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	lease, err := h.pool.Acquire(r.Context())
	if err != nil {
		if errors.Is(err, ErrNoCapacity) {
			http.Error(w, "no upstream capacity", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer lease.release()

	targetURL, err := url.Parse(lease.node.address)
	if err != nil {
		http.Error(w, "invalid upstream address", http.StatusBadGateway)
		return
	}

	proxy := &httputil.ReverseProxy{
		Transport:     lease.node.transport,
		FlushInterval: -1,
		ErrorHandler:  proxyErrorHandler,
		Director:      proxyDirector(targetURL),
	}
	var verification *shadowVerification
	if h.shadows != nil {
		verification = h.shadows.Start(r)
	}
	if verification != nil {
		recorder := &responseRecorder{
			ResponseWriter: w,
			maxBytes:       maxComparisonBody + 1,
		}
		defer func() {
			verification.completePrimary(recorder.result(), recorder.err != nil || r.Context().Err() != nil)
		}()
		proxy.ServeHTTP(recorder, r)
		return
	}
	proxy.ServeHTTP(w, r)
}

func proxyDirector(target *url.URL) func(*http.Request) {
	return func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		req.RequestURI = ""
		removeHopHeaders(req.Header)
	}
}

func proxyErrorHandler(w http.ResponseWriter, _ *http.Request, err error) {
	http.Error(w, "bad gateway: "+err.Error(), http.StatusBadGateway)
}

type responseRecorder struct {
	http.ResponseWriter
	status    int
	body      []byte
	maxBytes  int
	err       error
	wroteHead bool
	oversized bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if !r.wroteHead {
		r.status = status
		r.wroteHead = true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	r.record(data)
	n, err := r.ResponseWriter.Write(data)
	if err != nil {
		r.err = err
	}
	return n, err
}

func (r *responseRecorder) ReadFrom(reader io.Reader) (int64, error) {
	if writer, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		return writer.ReadFrom(&recordingReader{reader: reader, recorder: r})
	}
	return io.Copy(&fallbackWriter{responseWriter: r.ResponseWriter, recorder: r}, reader)
}

func (r *responseRecorder) Flush() {
	if !r.wroteHead {
		r.WriteHeader(http.StatusOK)
	}
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *responseRecorder) record(data []byte) {
	if !r.wroteHead {
		r.WriteHeader(http.StatusOK)
	}
	if len(r.body) < r.maxBytes {
		remaining := r.maxBytes - len(r.body)
		if len(data) <= remaining {
			r.body = append(r.body, data...)
		} else {
			r.body = append(r.body, data[:remaining]...)
			r.oversized = true
		}
	}
}

type recordingReader struct {
	reader   io.Reader
	recorder *responseRecorder
}

func (r *recordingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.recorder.record(p[:n])
	}
	return n, err
}

type fallbackWriter struct {
	responseWriter io.Writer
	recorder       *responseRecorder
}

func (w *fallbackWriter) Write(p []byte) (int, error) {
	w.recorder.record(p)
	return w.responseWriter.Write(p)
}

func (r *responseRecorder) result() capturedBody {
	if r.err != nil {
		return capturedBody{status: r.status, reason: "primary response stream interrupted: " + r.err.Error()}
	}
	if r.oversized || len(r.body) > maxComparisonBody {
		return capturedBody{status: r.status, reason: "primary response body exceeds 1 MiB"}
	}
	return capturedBody{status: r.status, body: r.body}
}
