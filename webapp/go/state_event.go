package main

import (
	"encoding/json"
	"fmt"
	"sort"
)

const stateEventVersion = 1

type statePayload struct {
	Core      stateCore      `json:"core"`
	Inventory stateInventory `json:"inventory"`
	Inbox     stateInbox     `json:"inbox"`
}

type stateDelta struct {
	Version        int                              `json:"v"`
	Create         *statePayload                    `json:"create,omitempty"`
	User           *User                            `json:"user,omitempty"`
	Banned         *bool                            `json:"banned,omitempty"`
	Devices        []*UserDevice                    `json:"devices,omitempty"`
	Decks          []*UserDeck                      `json:"decks,omitempty"`
	Bonuses        []*UserLoginBonus                `json:"bonuses,omitempty"`
	History        []*UserPresentAllReceivedHistory `json:"history,omitempty"`
	TokenChanged   bool                             `json:"tokenChanged,omitempty"`
	Token          *UserOneTimeToken                `json:"token,omitempty"`
	SessionChanged bool                             `json:"sessionChanged,omitempty"`
	Session        *Session                         `json:"session,omitempty"`
	Cards          []*UserCard                      `json:"cards,omitempty"`
	Items          []*UserItem                      `json:"items,omitempty"`
	Dynamic        []*UserPresent                   `json:"dynamic,omitempty"`
	Received       map[int64]int64                  `json:"received,omitempty"`
}

func upsertRows[T any](dst []*T, changes []*T, id func(*T) int64) []*T {
	if len(changes) == 0 {
		return dst
	}
	positions := make(map[int64]int, len(dst))
	for i, row := range dst {
		positions[id(row)] = i
	}
	for _, row := range changes {
		if i, ok := positions[id(row)]; ok {
			dst[i] = row
		} else {
			positions[id(row)] = len(dst)
			dst = append(dst, row)
		}
	}
	return dst
}

func buildStateDelta(st *userState) stateDelta {
	if st.base == nil {
		return stateDelta{Version: stateEventVersion, Create: &statePayload{Core: st.Core, Inventory: st.Inventory, Inbox: st.Inbox}}
	}
	d := st.changes.delta
	d.Version = stateEventVersion
	return d
}

func applyStateDelta(st *userState, d stateDelta) error {
	if d.Version != stateEventVersion {
		return fmt.Errorf("unknown state event version: %d", d.Version)
	}
	if d.Create != nil {
		st.Core, st.Inventory, st.Inbox = d.Create.Core, d.Create.Inventory, d.Create.Inbox
	} else {
		if d.User != nil {
			st.Core.User = d.User
		}
		if d.Banned != nil {
			st.Core.Banned = *d.Banned
		}
		st.Core.Devices = upsertRows(st.Core.Devices, d.Devices, func(v *UserDevice) int64 { return v.ID })
		st.Core.Decks = upsertRows(st.Core.Decks, d.Decks, func(v *UserDeck) int64 { return v.ID })
		st.Core.LoginBonuses = upsertRows(st.Core.LoginBonuses, d.Bonuses, func(v *UserLoginBonus) int64 { return v.ID })
		st.Core.PresentHistory = upsertRows(st.Core.PresentHistory, d.History, func(v *UserPresentAllReceivedHistory) int64 { return v.ID })
		if d.TokenChanged {
			st.Core.Token = d.Token
		}
		if d.SessionChanged {
			st.Core.Session = d.Session
		}
		st.Inventory.Cards = upsertRows(st.Inventory.Cards, d.Cards, func(v *UserCard) int64 { return v.ID })
		st.Inventory.Items = upsertRows(st.Inventory.Items, d.Items, func(v *UserItem) int64 { return v.ID })
		st.Inbox.Dynamic = upsertRows(st.Inbox.Dynamic, d.Dynamic, func(v *UserPresent) int64 { return v.ID })
		if st.Inbox.Received == nil {
			st.Inbox.Received = make(map[int64]int64)
		}
		for id, at := range d.Received {
			st.Inbox.Received[id] = at
		}
	}
	sortDynamicPresents(st.Inbox.Dynamic)
	return nil
}

func sortDynamicPresents(presents []*UserPresent) {
	sort.Slice(presents, func(i, j int) bool {
		if presents[i].CreatedAt != presents[j].CreatedAt {
			return presents[i].CreatedAt > presents[j].CreatedAt
		}
		return presents[i].ID < presents[j].ID
	})
}

type eventAppend struct {
	userID     int64
	seq        int64
	payload    []byte
	oldSession *Session
	newSession *Session
}

func newEventAppend(st *userState) (*eventAppend, error) {
	d := buildStateDelta(st)
	payload, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	req := &eventAppend{userID: st.ID, seq: st.Revision + 1, payload: payload}
	if st.base == nil {
		req.newSession = st.Core.Session
	} else if d.SessionChanged {
		req.oldSession, req.newSession = st.base.Core.Session, st.Core.Session
	}
	return req, nil
}
