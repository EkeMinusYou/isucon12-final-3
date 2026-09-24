package main

import "testing"

func TestStateCacheKeepsCommittedSnapshot(t *testing.T) {
	cache := newStateStore()
	st := &userState{ID: 17, Revision: 3, Core: stateCore{User: &User{ID: 17, IsuCoin: 100}, Banned: true},
		Inbox: stateInbox{Received: map[int64]int64{5: 10}}}
	cache.putOwned(st)
	got, ok := cache.get(17)
	if !ok {
		t.Fatal("committed state missing from cache")
	}
	got.Core.User.IsuCoin = 200
	got.Inbox.Received[5] = 20
	got, ok = cache.get(17)
	if !ok {
		t.Fatal("committed state missing from cache")
	}
	if got.Revision != 3 || got.Core.User.IsuCoin != 100 || got.Inbox.Received[5] != 10 || !got.Core.Banned {
		t.Fatalf("cache exposed uncommitted mutation: %+v", got)
	}
	got.Core.User.IsuCoin = 300
	again, ok := cache.get(17)
	if !ok || again.Core.User.IsuCoin != 100 {
		t.Fatalf("cache exposed reader mutation: %+v", again)
	}
}

func TestCombinedPresentsOverlayAndOrdering(t *testing.T) {
	st := &userState{Inbox: stateInbox{Received: map[int64]int64{2: 90},
		Dynamic: []*UserPresent{{ID: 100000000001, CreatedAt: 20}, {ID: 100000000002, CreatedAt: 10}}}}
	seed := []*UserPresent{{ID: 1, CreatedAt: 10, UpdatedAt: 10}, {ID: 2, CreatedAt: 10, UpdatedAt: 10}}
	active := combinedPresents(seed, st, false)
	if len(active) != 3 || active[0].ID != 100000000001 || active[1].ID != 1 || active[2].ID != 100000000002 {
		t.Fatalf("unexpected active presents: %+v", active)
	}
	all := combinedPresents(seed, st, true)
	if len(all) != 4 || all[2].ID != 2 || all[2].DeletedAt == nil ||
		*all[2].DeletedAt != 90 || all[2].UpdatedAt != 90 {
		t.Fatalf("receipt overlay missing: %+v", all)
	}
	if seed[1].DeletedAt != nil {
		t.Fatal("seed row was mutated")
	}
}

func TestStateTokenUseOnce(t *testing.T) {
	st := &userState{Core: stateCore{Token: &UserOneTimeToken{Token: "abc", TokenType: 2, ExpiredAt: 100}}}
	if err := stateTokenValid(st, "abc", 2, 100); err != nil {
		t.Fatal(err)
	}
	if err := stateTokenValid(st, "abc", 2, 100); err != ErrInvalidToken {
		t.Fatalf("second use: %v", err)
	}
}
