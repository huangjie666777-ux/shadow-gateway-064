package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type UpstreamConfig struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Limit   int    `json:"limit"`
}

type ConfigSnapshot struct {
	Version   int              `json:"version"`
	Upstreams []UpstreamConfig `json:"upstreams"`
}

type ShadowPolicy struct {
	Enabled          bool   `json:"enabled"`
	CandidateAddress string `json:"candidate_address"`
	PathPrefix       string `json:"path_prefix"`
	MaxConcurrency   int    `json:"max_concurrency"`
	TimeoutMillis    int    `json:"timeout_ms"`
}

type ShadowPolicySnapshot struct {
	ShadowPolicy
	Version int `json:"version"`
}

func validateUpstreams(input []UpstreamConfig) error {
	seen := make(map[string]struct{}, len(input))
	for i, upstream := range input {
		if strings.TrimSpace(upstream.ID) == "" {
			return fmt.Errorf("upstreams[%d].id must be non-empty", i)
		}
		if _, exists := seen[upstream.ID]; exists {
			return fmt.Errorf("upstream id %q is duplicated", upstream.ID)
		}
		seen[upstream.ID] = struct{}{}

		if upstream.Limit <= 0 {
			return fmt.Errorf("upstream %q limit must be a positive integer", upstream.ID)
		}

		if err := validateHTTPAddress(upstream.Address, "upstream "+upstream.ID+" address"); err != nil {
			return err
		}
	}
	return nil
}

func validateHTTPAddress(address, field string) error {
	parsed, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("%s is invalid: %w", field, err)
	}
	if parsed.Scheme != "http" {
		return fmt.Errorf("%s must use http", field)
	}
	if parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawFragment != "" {
		return fmt.Errorf("%s must only contain host and port", field)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("%s must contain a host and port", field)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("%s port must be between 1 and 65535", field)
	}
	if parsed.Host != net.JoinHostPort(host, port) {
		return fmt.Errorf("%s must contain a host and port", field)
	}
	return nil
}

func validateShadowPolicy(input ShadowPolicy) error {
	if err := validateHTTPAddress(input.CandidateAddress, "candidate_address"); err != nil {
		return err
	}
	if !strings.HasPrefix(input.PathPrefix, "/") {
		return errors.New("path_prefix must start with a slash")
	}
	if input.MaxConcurrency <= 0 {
		return errors.New("max_concurrency must be a positive integer")
	}
	if input.TimeoutMillis <= 0 {
		return errors.New("timeout_ms must be a positive integer")
	}
	return nil
}

func decodeUpstreams(r *http.Request) ([]UpstreamConfig, error) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > 1<<20 {
		return nil, fmt.Errorf("request body exceeds 1 MiB")
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, fmt.Errorf("request body must be a JSON array")
	}
	var upstreams []UpstreamConfig
	var rawItems []json.RawMessage
	if err := json.Unmarshal(trimmed, &rawItems); err != nil {
		return nil, err
	}
	upstreams = make([]UpstreamConfig, 0, len(rawItems))
	for _, raw := range rawItems {
		itemDecoder := json.NewDecoder(bytes.NewReader(raw))
		itemDecoder.DisallowUnknownFields()
		var upstream UpstreamConfig
		if err := itemDecoder.Decode(&upstream); err != nil {
			return nil, err
		}
		upstreams = append(upstreams, upstream)
	}
	arrayDecoder := json.NewDecoder(bytes.NewReader(trimmed))
	var array any
	if err := arrayDecoder.Decode(&array); err != nil {
		return nil, err
	}
	if err := arrayDecoder.Decode(&array); err != io.EOF {
		return nil, fmt.Errorf("request body must be a single JSON array")
	}
	return upstreams, nil
}
