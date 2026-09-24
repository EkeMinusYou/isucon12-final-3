package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRouteMiddlewareForwardsUserRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		url    string
		path   string
		userID string
		body   string
		owner  string
	}{
		{"user API", http.MethodPost, "/user/100000000002/card?mode=one", "/user/:userID/card", "100000000002", `{"cardIds":[1]}`, "10.0.0.2"},
		{"login", http.MethodPost, "/login", "/login", "", `{"userId":100000000003,"viewerId":"v"}`, "10.0.0.3"},
		{"admin user", http.MethodGet, "/admin/user/100000000004", "/admin/user/:userID", "100000000004", "", "10.0.0.4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			cluster := &clusterTopology{Hosts: []clusterHost{
				{IP: "10.0.0.1"}, {IP: "10.0.0.2"}, {IP: "10.0.0.3"}, {IP: "10.0.0.4"}, {IP: "10.0.0.5"},
			}, Self: 0}
			cluster.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				called = true
				if req.URL.Host != tt.owner+":8080" || req.URL.RequestURI() != tt.url {
					t.Errorf("forwarded to %s%s", req.URL.Host, req.URL.RequestURI())
				}
				if req.Header.Get("X-Session") != "session" || req.Header.Get("X-Drop") != "" || req.Header.Get("X-Isu-Internal-Hop") != "1" {
					t.Errorf("forwarded headers = %v", req.Header)
				}
				body, err := io.ReadAll(req.Body)
				if err != nil || string(body) != tt.body {
					t.Errorf("forwarded body = %q, err = %v", body, err)
				}
				return &http.Response{StatusCode: http.StatusAccepted, Header: http.Header{
					"X-Measurement-Session-Id": {"hash"}, "Connection": {"X-Drop"}, "X-Drop": {"drop"},
				}, Body: io.NopCloser(strings.NewReader("forwarded"))}, nil
			})}
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.url, strings.NewReader(tt.body))
			req.Header.Set("X-Session", "session")
			req.Header.Set("Connection", "X-Drop")
			req.Header.Set("X-Drop", "drop")
			rec := httptest.NewRecorder()
			ctx := e.NewContext(req, rec)
			ctx.SetPath(tt.path)
			ctx.SetParamNames("userID")
			ctx.SetParamValues(tt.userID)
			err := cluster.routeMiddleware(func(c echo.Context) error {
				t.Error("request reached local handler")
				return nil
			})(ctx)
			if err != nil || !called {
				t.Fatalf("forward err = %v, called = %v", err, called)
			}
			if rec.Code != http.StatusAccepted || rec.Body.String() != "forwarded" || rec.Header().Get("X-Measurement-Session-Id") != "hash" || rec.Header().Get("X-Drop") != "" {
				t.Errorf("response: code=%d body=%q headers=%v", rec.Code, rec.Body.String(), rec.Header())
			}
		})
	}
}

func TestRouteMiddlewareLeavesOwnedRequestLocal(t *testing.T) {
	cluster := &clusterTopology{Hosts: make([]clusterHost, 5), Self: 1}
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"userId":100000000002}`))
	ctx := e.NewContext(req, httptest.NewRecorder())
	ctx.SetPath("/login")
	called := false
	err := cluster.routeMiddleware(func(c echo.Context) error {
		called = true
		body, err := io.ReadAll(c.Request().Body)
		if err != nil || string(body) != `{"userId":100000000002}` {
			t.Errorf("local body = %q, err = %v", body, err)
		}
		return nil
	})(ctx)
	if err != nil || !called {
		t.Fatalf("local handler err = %v, called = %v", err, called)
	}
}

func TestWaitForWorkersRetriesUntilHealthy(t *testing.T) {
	cluster := &clusterTopology{Hosts: []clusterHost{
		{IP: "10.0.0.1"}, {Name: "isucon-2", IP: "10.0.0.2"}, {Name: "isucon-3", IP: "10.0.0.3"},
	}}
	attempts := make(map[string]int)
	cluster.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts[req.URL.Host]++
		status := http.StatusOK
		if req.URL.Host == "10.0.0.2:8080" && attempts[req.URL.Host] == 1 {
			status = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if err := cluster.waitForWorkers(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts["10.0.0.2:8080"] != 2 || attempts["10.0.0.3:8080"] != 1 {
		t.Fatalf("health attempts = %v", attempts)
	}
}
