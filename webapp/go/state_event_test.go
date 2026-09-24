package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestStateDeltaReplaysCommittedChanges(t *testing.T) {
	base := &userState{ID: 7, Revision: 5,
		Core:      stateCore{User: &User{ID: 7, IsuCoin: 100}, Session: &Session{UserID: 7, SessionID: "old"}, Token: &UserOneTimeToken{Token: "once"}},
		Inventory: stateInventory{Items: []*UserItem{{ID: 10, UserID: 7, Amount: 1}}},
		Inbox:     stateInbox{Dynamic: []*UserPresent{{ID: 100000000001, UserID: 7, CreatedAt: 1}}, Received: map[int64]int64{2: 3}},
	}
	working := newWorkingState(base)
	working.editUser().IsuCoin = 120
	working.setSession(&Session{UserID: 7, SessionID: "new"})
	working.setToken(nil)
	working.editItem(10).Amount = 4
	working.setReceived(5, 9)
	deletedAt := int64(9)
	working.editDynamic(100000000001).DeletedAt = &deletedAt
	working.addDynamic(&UserPresent{ID: 100000000002, UserID: 7, CreatedAt: 10})

	delta := buildStateDelta(working)
	encoded, err := json.Marshal(delta)
	if err != nil {
		t.Fatal(err)
	}
	var decoded stateDelta
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	recovered := copyStateForTest(t, base)
	if err := applyStateDelta(recovered, decoded); err != nil {
		t.Fatal(err)
	}
	sortDynamicPresents(working.Inbox.Dynamic)
	if !reflect.DeepEqual(recovered.Core, working.Core) || !reflect.DeepEqual(recovered.Inventory, working.Inventory) || !reflect.DeepEqual(recovered.Inbox, working.Inbox) {
		t.Fatalf("replayed state differs: recovered=%+v working=%+v", recovered, working)
	}
	if base.Core.User.IsuCoin != 100 || base.Inventory.Items[0].Amount != 1 || base.Inbox.Dynamic[0].DeletedAt != nil || len(base.Inbox.Dynamic) != 1 {
		t.Fatal("uncommitted mutation reached the base state")
	}
}

func copyStateForTest(t *testing.T, st *userState) *userState {
	t.Helper()
	payload, err := json.Marshal(statePayload{Core: st.Core, Inventory: st.Inventory, Inbox: st.Inbox})
	if err != nil {
		t.Fatal(err)
	}
	var copy statePayload
	if err := json.Unmarshal(payload, &copy); err != nil {
		t.Fatal(err)
	}
	return &userState{ID: st.ID, Revision: st.Revision, Core: copy.Core, Inventory: copy.Inventory, Inbox: copy.Inbox}
}

func TestCreateEventRestoresSessionAndInventory(t *testing.T) {
	created := &userState{ID: 42, Core: stateCore{User: &User{ID: 42}, Session: &Session{UserID: 42, SessionID: "first"}},
		Inventory: stateInventory{Cards: []*UserCard{{ID: 10, UserID: 42}}}, Inbox: stateInbox{Received: map[int64]int64{}}}
	delta := buildStateDelta(created)
	if delta.Create == nil {
		t.Fatal("missing create event")
	}
	recovered := &userState{ID: 42}
	if err := applyStateDelta(recovered, delta); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered.Core, created.Core) || !reflect.DeepEqual(recovered.Inventory, created.Inventory) {
		t.Fatal("create event lost state")
	}
}

func TestTokenConsumptionAndPresentReceiptReplay(t *testing.T) {
	base := &userState{ID: 7, Core: stateCore{Token: &UserOneTimeToken{Token: "once", TokenType: 1, ExpiredAt: 100}},
		Inbox: stateInbox{Dynamic: []*UserPresent{{ID: 100000000001, UserID: 7, CreatedAt: 10}}, Received: map[int64]int64{}}}
	working := newWorkingState(base)
	if err := stateTokenValid(working, "once", 1, 100); err != nil {
		t.Fatal(err)
	}
	first := buildStateDelta(working)
	if !first.TokenChanged || first.Token != nil {
		t.Fatal("token consumption was not recorded")
	}
	firstCommitted := working.promoteCommitted(10)
	if err := stateTokenValid(working, "once", 1, 100); err != ErrInvalidToken {
		t.Fatalf("token reused after first commit: %v", err)
	}
	recovered := copyStateForTest(t, base)
	if err := applyStateDelta(recovered, first); err != nil {
		t.Fatal(err)
	}
	if err := stateTokenValid(recovered, "once", 1, 100); err != ErrInvalidToken {
		t.Fatalf("token reused after replay: %v", err)
	}

	working.setReceived(2, 100)
	deletedAt := int64(100)
	working.editDynamic(100000000001).DeletedAt = &deletedAt
	second := buildStateDelta(working)
	if second.TokenChanged {
		t.Fatal("previous token change leaked into the next event")
	}
	if firstCommitted.Inbox.Dynamic[0].DeletedAt != nil || len(firstCommitted.Inbox.Received) != 0 {
		t.Fatal("second event mutated the first committed state")
	}
	if err := applyStateDelta(recovered, second); err != nil {
		t.Fatal(err)
	}
	seed := []*UserPresent{{ID: 2, UserID: 7, CreatedAt: 10}}
	if active := combinedPresents(seed, recovered, false); len(active) != 0 {
		t.Fatalf("received presents became available again: %+v", active)
	}
	if base.Core.Token == nil || base.Inbox.Dynamic[0].DeletedAt != nil || len(base.Inbox.Received) != 0 {
		t.Fatal("uncommitted changes reached the base state")
	}
}
