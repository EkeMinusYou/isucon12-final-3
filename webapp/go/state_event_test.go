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
	working := cloneUserState(base)
	working.base = base
	working.Core.User.IsuCoin = 120
	working.Core.Session = &Session{UserID: 7, SessionID: "new"}
	working.Core.Token = nil
	working.Inventory.Items[0].Amount = 4
	working.Inbox.Received[5] = 9
	working.Inbox.Dynamic[0].DeletedAt = new(int64)
	*working.Inbox.Dynamic[0].DeletedAt = 9
	working.Inbox.Dynamic = append(working.Inbox.Dynamic, &UserPresent{ID: 100000000002, UserID: 7, CreatedAt: 10})

	delta := buildStateDelta(working, true, true, true)
	encoded, err := json.Marshal(delta)
	if err != nil {
		t.Fatal(err)
	}
	var decoded stateDelta
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	recovered := cloneUserState(base)
	if err := applyStateDelta(recovered, decoded); err != nil {
		t.Fatal(err)
	}
	sortDynamicPresents(working.Inbox.Dynamic)
	if !reflect.DeepEqual(recovered.Core, working.Core) || !reflect.DeepEqual(recovered.Inventory, working.Inventory) || !reflect.DeepEqual(recovered.Inbox, working.Inbox) {
		t.Fatalf("replayed state differs: recovered=%+v working=%+v", recovered, working)
	}
	if base.Core.User.IsuCoin != 100 || base.Inventory.Items[0].Amount != 1 || base.Inbox.Dynamic[0].DeletedAt != nil {
		t.Fatal("uncommitted mutation reached the base state")
	}
}

func TestCreateEventRestoresSessionAndInventory(t *testing.T) {
	created := &userState{ID: 42, Core: stateCore{User: &User{ID: 42}, Session: &Session{UserID: 42, SessionID: "first"}},
		Inventory: stateInventory{Cards: []*UserCard{{ID: 10, UserID: 42}}}, Inbox: stateInbox{Received: map[int64]int64{}}}
	delta := buildStateDelta(created, true, true, true)
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
