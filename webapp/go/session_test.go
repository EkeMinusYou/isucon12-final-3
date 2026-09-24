package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

type sessionLookupTestDriver struct{}

var registerSessionLookupTestDriver sync.Once

func (sessionLookupTestDriver) Open(name string) (driver.Conn, error) {
	return &sessionLookupTestConn{shard: name}, nil
}

type sessionLookupTestConn struct{ shard string }

func (*sessionLookupTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (*sessionLookupTestConn) Close() error { return nil }
func (*sessionLookupTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (c *sessionLookupTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT user_id FROM user_session_current WHERE session_id=?" || len(args) != 1 {
		return nil, errors.New("unexpected session query")
	}
	return &sessionLookupTestRows{found: c.shard == "remote" && args[0].Value == "valid-session"}, nil
}

type sessionLookupTestRows struct{ found bool }

func (*sessionLookupTestRows) Columns() []string { return []string{"user_id"} }
func (*sessionLookupTestRows) Close() error      { return nil }
func (r *sessionLookupTestRows) Next(dest []driver.Value) error {
	if !r.found {
		return io.EOF
	}
	r.found = false
	dest[0] = int64(100000000002)
	return nil
}

func TestCheckSessionFindsOtherShard(t *testing.T) {
	registerSessionLookupTestDriver.Do(func() {
		sql.Register("session-lookup-test", sessionLookupTestDriver{})
	})
	open := func(shard string) *sqlx.DB {
		raw, err := sql.Open("session-lookup-test", shard)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { raw.Close() })
		return sqlx.NewDb(raw, "session-lookup-test")
	}
	local, remote := open("local"), open("remote")
	h := &Handler{DB: local, SessionDBs: []*sqlx.DB{local, remote}, Cluster: &clusterTopology{Self: 0}, State: newStateStore()}
	for _, tt := range []struct {
		name, session string
		want          int
	}{
		{"other user's session", "valid-session", http.StatusForbidden},
		{"unknown session", "missing-session", http.StatusUnauthorized},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/user/0/home", nil)
			req.Header.Set("X-Session", tt.session)
			rec := httptest.NewRecorder()
			ctx := e.NewContext(req, rec)
			ctx.SetParamNames("userID")
			ctx.SetParamValues("0")
			ctx.Set("requestTime", int64(1))
			err := h.checkSessionMiddleware(func(echo.Context) error {
				t.Fatal("request reached protected handler")
				return nil
			})(ctx)
			if err != nil || rec.Code != tt.want {
				t.Fatalf("status = %d, err = %v; want %d", rec.Code, err, tt.want)
			}
		})
	}
}
