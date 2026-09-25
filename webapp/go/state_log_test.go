package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
)

func TestStateLogPeriodicallySyncsAcknowledgedEvents(t *testing.T) {
	dir := t.TempDir()
	w, _, _, err := newEventWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(mustEvent(t, &userState{ID: 42, Core: stateCore{User: &User{ID: 42}}})); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		unsynced, failed := w.unsynced, w.failed
		w.mu.Unlock()
		if failed != nil {
			t.Fatal(failed)
		}
		if !unsynced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("acknowledged event was not synced")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	reopened, recovered, _, err := newEventWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if st := recovered[42]; st == nil || st.Revision != 1 {
		t.Fatalf("synced event was not recovered: %+v", st)
	}
}

func TestStagedTokenCommitRecoversSuccessAndFailure(t *testing.T) {
	for _, success := range []bool{true, false} {
		name := "failure"
		if success {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w, _, _, err := newEventWriter(dir)
			if err != nil {
				t.Fatal(err)
			}
			initial := &userState{ID: 42, Core: stateCore{
				User:  &User{ID: 42, IsuCoin: 100},
				Token: &UserOneTimeToken{UserID: 42, Token: "once", TokenType: 2, ExpiredAt: 100},
			}, Inventory: stateInventory{Cards: []*UserCard{{ID: 10, UserID: 42, Level: 1}}}}
			states := map[int64]*userState{42: initial}
			if err := w.reset(states, nil); err != nil {
				t.Fatal(err)
			}
			store := newStateStore()
			store.replace(states, nil)
			h := &Handler{State: store, Writer: w}
			working, _ := store.get(42)
			tokenOnly, err := h.stageStateToken(working, "once", 2, 100)
			if err != nil {
				t.Fatal(err)
			}
			working.editCard(10).Level = 2
			if success {
				working.editUser().IsuCoin = 125
				if err := h.saveUserState(working); err != nil {
					t.Fatal(err)
				}
			} else {
				e := echo.New()
				c := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), httptest.NewRecorder())
				if err := h.consumedTokenError(c, tokenOnly, http.StatusBadRequest, errors.New("invalid item")); err != nil {
					t.Fatal(err)
				}
				if c.Response().Status != http.StatusBadRequest {
					t.Fatalf("status = %d", c.Response().Status)
				}
			}
			if err := w.close(); err != nil {
				t.Fatal(err)
			}
			reopened, recovered, _, err := newEventWriter(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.close()
			got := recovered[42]
			if got == nil || got.Revision != 1 || got.Core.Token != nil {
				t.Fatalf("token was not consumed in one event: %+v", got)
			}
			wantLevel, wantCoins := 1, int64(100)
			if success {
				wantLevel, wantCoins = 2, 125
			}
			if got.Inventory.Cards[0].Level != wantLevel || got.Core.User.IsuCoin != wantCoins {
				t.Fatalf("recovered unrelated changes: level=%d coins=%d", got.Inventory.Cards[0].Level, got.Core.User.IsuCoin)
			}
		})
	}
}

func TestConcurrentStateLogResponsesAreRecoverable(t *testing.T) {
	dir := t.TempDir()
	w, _, _, err := newEventWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()

	const users = 64
	requests := make([]*eventAppend, users)
	for i := range requests {
		id := int64(i + 1)
		requests[i] = mustEvent(t, &userState{ID: id, Core: stateCore{User: &User{ID: id}}})
	}
	start := make(chan struct{})
	results := make(chan error, users)
	for _, req := range requests {
		go func(req *eventAppend) {
			<-start
			results <- w.append(req)
		}(req)
	}
	close(start)
	for range requests {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}

	recovered, _, err := recoverLocalState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != users {
		t.Fatalf("recovered %d users, want %d", len(recovered), users)
	}
	for i := 1; i <= users; i++ {
		if st := recovered[int64(i)]; st == nil || st.Revision != 1 {
			t.Fatalf("missing committed user %d: %+v", i, st)
		}
	}
}

func TestLocalStateLogSurvivesCheckpointAndRestart(t *testing.T) {
	dir := t.TempDir()
	w, _, _, err := newEventWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	initial := &userState{ID: 42, Core: stateCore{
		User:    &User{ID: 42, IsuCoin: 100},
		Session: &Session{UserID: 42, SessionID: "old", ExpiredAt: 100},
		Token:   &UserOneTimeToken{UserID: 42, Token: "once", TokenType: 1, ExpiredAt: 100},
	}, Inbox: stateInbox{Dynamic: []*UserPresent{{ID: 100000000001, UserID: 42, CreatedAt: 10}}}}
	states := map[int64]*userState{42: initial}
	seeds := map[int64][]*UserPresent{42: {{ID: 2, UserID: 42, CreatedAt: 10}}}
	if err := w.reset(states, seeds); err != nil {
		t.Fatal(err)
	}
	store := newStateStore()
	store.replace(states, seeds)
	h := &Handler{State: store, Writer: w}
	working, ok := store.get(42)
	if !ok {
		t.Fatal("missing initial state")
	}
	if err := stateTokenValid(working, "once", 1, 100); err != nil {
		t.Fatal(err)
	}
	working.editUser().IsuCoin = 125
	working.setReceived(2, 20)
	deletedAt := int64(20)
	working.editDynamic(100000000001).DeletedAt = &deletedAt
	working.setSession(&Session{UserID: 42, SessionID: "new", ExpiredAt: 200})
	if err := h.saveUserState(working); err != nil {
		t.Fatal(err)
	}
	if owner, ok := store.sessionOwner("new"); !ok || owner != 42 {
		t.Fatal("new session was not indexed")
	}
	if _, ok := store.sessionOwner("old"); ok {
		t.Fatal("old session remained indexed")
	}
	if err := h.checkpointLocal(); err != nil {
		t.Fatal(err)
	}
	working, _ = store.get(42)
	working.setBanned(true)
	if err := h.saveUserState(working); err != nil {
		t.Fatal(err)
	}
	want, _ := store.peek(42)
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	reopened, recovered, recoveredSeeds, err := newEventWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	got := recovered[42]
	if got == nil || got.Revision != want.Revision || !reflect.DeepEqual(got.Core, want.Core) || !reflect.DeepEqual(got.Inbox, want.Inbox) {
		t.Fatalf("recovered state differs: got=%+v want=%+v", got, want)
	}
	if err := stateTokenValid(got, "once", 1, 100); err != ErrInvalidToken {
		t.Fatalf("token reused after restart: %v", err)
	}
	if !reflect.DeepEqual(recoveredSeeds, seeds) {
		t.Fatalf("seed presents changed: got=%+v", recoveredSeeds)
	}
	if active := combinedPresents(recoveredSeeds[42], got, false); len(active) != 0 {
		t.Fatalf("received presents returned after restart: %+v", active)
	}
	store2 := newStateStore()
	store2.replace(recovered, nil)
	if owner, ok := store2.sessionOwner("new"); !ok || owner != 42 {
		t.Fatal("session index was not rebuilt")
	}
}

func TestLocalStateLogTruncatesIncompleteTail(t *testing.T) {
	dir := t.TempDir()
	w, _, _, err := newEventWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	created := &userState{ID: 42, Core: stateCore{User: &User{ID: 42}}}
	if err := w.append(mustEvent(t, created)); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(logPath(dir, 0), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{5, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, states, _, err := newEventWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if states[42] == nil || states[42].Revision != 1 {
		t.Fatalf("committed event was lost: %+v", states[42])
	}
}

func mustEvent(t *testing.T, st *userState) *eventAppend {
	t.Helper()
	req, err := newEventAppend(st)
	if err != nil {
		t.Fatal(err)
	}
	return req
}
