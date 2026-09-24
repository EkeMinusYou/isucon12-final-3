package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	stateEventVersion   = 1
	stateBatchSize      = 32
	stateBatchDelay     = time.Millisecond
	stateSnapshotEvents = 64
	stateSnapshotBytes  = 64 << 10
)

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

func diffRows[T any](old, next []*T, id func(*T) int64) []*T {
	previous := make(map[int64]*T, len(old))
	for _, row := range old {
		previous[id(row)] = row
	}
	var changes []*T
	for _, row := range next {
		if !reflect.DeepEqual(previous[id(row)], row) {
			changes = append(changes, row)
		}
	}
	return changes
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

func buildStateDelta(st *userState, core, inventory, inbox bool) stateDelta {
	d := stateDelta{Version: stateEventVersion}
	old := st.base
	if old == nil {
		d.Create = &statePayload{Core: st.Core, Inventory: st.Inventory, Inbox: st.Inbox}
		return d
	}
	if core {
		if !reflect.DeepEqual(old.Core.User, st.Core.User) {
			d.User = st.Core.User
		}
		if old.Core.Banned != st.Core.Banned {
			value := st.Core.Banned
			d.Banned = &value
		}
		d.Devices = diffRows(old.Core.Devices, st.Core.Devices, func(v *UserDevice) int64 { return v.ID })
		d.Decks = diffRows(old.Core.Decks, st.Core.Decks, func(v *UserDeck) int64 { return v.ID })
		d.Bonuses = diffRows(old.Core.LoginBonuses, st.Core.LoginBonuses, func(v *UserLoginBonus) int64 { return v.ID })
		d.History = diffRows(old.Core.PresentHistory, st.Core.PresentHistory, func(v *UserPresentAllReceivedHistory) int64 { return v.ID })
		if !reflect.DeepEqual(old.Core.Token, st.Core.Token) {
			d.TokenChanged, d.Token = true, st.Core.Token
		}
		if !reflect.DeepEqual(old.Core.Session, st.Core.Session) {
			d.SessionChanged, d.Session = true, st.Core.Session
		}
	}
	if inventory {
		d.Cards = diffRows(old.Inventory.Cards, st.Inventory.Cards, func(v *UserCard) int64 { return v.ID })
		d.Items = diffRows(old.Inventory.Items, st.Inventory.Items, func(v *UserItem) int64 { return v.ID })
	}
	if inbox {
		d.Dynamic = diffRows(old.Inbox.Dynamic, st.Inbox.Dynamic, func(v *UserPresent) int64 { return v.ID })
		for id, at := range st.Inbox.Received {
			if oldAt, ok := old.Inbox.Received[id]; !ok || oldAt != at {
				if d.Received == nil {
					d.Received = make(map[int64]int64)
				}
				d.Received[id] = at
			}
		}
	}
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
	userID         int64
	seq            int64
	payload        []byte
	oldSession     *Session
	newSession     *Session
	sessionChanged bool
	done           chan error
}

type eventWriter struct {
	db    *sqlx.DB
	queue chan *eventAppend
}

func newEventWriter(db *sqlx.DB) *eventWriter {
	w := &eventWriter{db: db, queue: make(chan *eventAppend, 4096)}
	go w.run()
	return w
}

func (w *eventWriter) append(req *eventAppend) error {
	w.queue <- req
	return <-req.done
}

func (w *eventWriter) run() {
	for first := range w.queue {
		batch := []*eventAppend{first}
		timer := time.NewTimer(stateBatchDelay)
	collect:
		for len(batch) < stateBatchSize {
			select {
			case req := <-w.queue:
				batch = append(batch, req)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		sort.Slice(batch, func(i, j int) bool { return batch[i].userID < batch[j].userID })
		err := w.commit(batch)
		for _, req := range batch {
			req.done <- err
		}
	}
}

func (w *eventWriter) commit(batch []*eventAppend) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		last = w.commitOnce(batch)
		if last == nil {
			return nil
		}
		committed, err := w.confirm(batch)
		if err == nil && committed {
			return nil
		}
		if err == nil && !committed {
			time.Sleep(time.Millisecond)
			continue
		}
		return fmt.Errorf("confirm state event commit: %w (commit: %v)", err, last)
	}
	return last
}

func (w *eventWriter) commitOnce(batch []*eventAppend) error {
	tx, err := w.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	var query strings.Builder
	query.WriteString("INSERT INTO user_state_events(user_id,seq,payload) VALUES ")
	args := make([]interface{}, 0, len(batch)*3)
	for i, req := range batch {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteString("(?,?,?)")
		args = append(args, req.userID, req.seq, req.payload)
	}
	if _, err := tx.Exec(query.String(), args...); err != nil {
		return err
	}
	for _, req := range batch {
		if !req.sessionChanged {
			continue
		}
		if req.newSession == nil {
			if _, err := tx.Exec("DELETE FROM user_session_current WHERE user_id=?", req.userID); err != nil {
				return err
			}
			continue
		}
		result, err := tx.Exec("UPDATE user_session_current SET session_id=? WHERE user_id=?", req.newSession.SessionID, req.userID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			if _, err := tx.Exec("INSERT INTO user_session_current(user_id,session_id) VALUES (?,?)", req.userID, req.newSession.SessionID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (w *eventWriter) confirm(batch []*eventAppend) (bool, error) {
	for _, req := range batch {
		var payload []byte
		err := w.db.Get(&payload, "SELECT payload FROM user_state_events WHERE user_id=? AND seq=?", req.userID, req.seq)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !bytes.Equal(payload, req.payload) {
			return false, fmt.Errorf("state event payload conflict: user=%d seq=%d", req.userID, req.seq)
		}
	}
	return true, nil
}

func newEventAppend(st *userState, core, inventory, inbox bool) (*eventAppend, error) {
	d := buildStateDelta(st, core, inventory, inbox)
	payload, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	req := &eventAppend{userID: st.ID, seq: st.Revision + 1, payload: payload, done: make(chan error, 1)}
	if st.base == nil {
		req.newSession = st.Core.Session
		req.sessionChanged = st.Core.Session != nil
	} else if d.SessionChanged {
		req.oldSession, req.newSession, req.sessionChanged = st.base.Core.Session, st.Core.Session, true
	}
	return req, nil
}
