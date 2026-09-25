package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

type sessionRoundTrip func(*http.Request) (*http.Response, error)

func (f sessionRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestCheckSessionUsesCommittedState(t *testing.T) {
	store := newStateStore()
	store.replace(map[int64]*userState{
		42: {ID: 42, Core: stateCore{User: &User{ID: 42}, Session: &Session{UserID: 42, SessionID: "current", ExpiredAt: 100}}},
		43: {ID: 43, Core: stateCore{User: &User{ID: 43}, Session: &Session{UserID: 43, SessionID: "other", ExpiredAt: 100}}},
	}, nil)
	h := &Handler{Cluster: &clusterTopology{Self: 1}, State: store}
	for _, tt := range []struct {
		name, session string
		want          int
	}{
		{"current session", "current", http.StatusNoContent},
		{"other user's session", "other", http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/user/42/home", nil)
			req.Header.Set("X-Session", tt.session)
			rec := httptest.NewRecorder()
			ctx := e.NewContext(req, rec)
			ctx.SetParamNames("userID")
			ctx.SetParamValues("42")
			ctx.Set("requestTime", int64(1))
			err := h.checkSessionMiddleware(func(c echo.Context) error { return c.NoContent(http.StatusNoContent) })(ctx)
			if err != nil || rec.Code != tt.want {
				t.Fatalf("status = %d, err = %v; want %d", rec.Code, err, tt.want)
			}
		})
	}
}

func TestRemoteSessionOwnerLookup(t *testing.T) {
	store := newStateStore()
	store.replace(nil, nil)
	cluster := &clusterTopology{Self: 1, Hosts: []clusterHost{
		{Name: "isucon-1", IP: "127.0.0.1"}, {Name: "isucon-2", IP: "127.0.0.2"}, {Name: "isucon-3", IP: "127.0.0.3"},
	}}
	cluster.client = &http.Client{Transport: sessionRoundTrip(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/_internal/session/owner" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil || string(body) != "remote-session" {
			t.Fatalf("unexpected lookup body: %q, %v", body, err)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("43")), Header: make(http.Header)}, nil
	})}
	h := &Handler{Cluster: cluster, State: store}
	owner, ok, err := h.findSessionOwner("remote-session")
	if err != nil || !ok || owner != 43 {
		t.Fatalf("owner=%d ok=%v err=%v", owner, ok, err)
	}
}
