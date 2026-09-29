package main

import (
	"errors"
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
	removeHopHeaders(r.Header)
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
	verification, shadowWriter, shadowing := h.shadows.Start(r)
	if shadowing {
		shadowWriter.ResponseWriter = w
		w = shadowWriter
		defer func() { verification.finishPrimary(shadowWriter, shadowWriter.writeErr) }()
	}

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

var hopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}
