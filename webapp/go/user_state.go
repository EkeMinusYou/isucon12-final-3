package main

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/labstack/echo/v4"
)

const (
	stateCacheEntries   = 256
	stateCacheBytes     = 8 << 20
	seedCacheBytes      = 8 << 20
	sessionCacheEntries = 16384
)

type stateCore struct {
	User           *User                            `json:"user"`
	Banned         bool                             `json:"banned"`
	Devices        []*UserDevice                    `json:"devices"`
	Decks          []*UserDeck                      `json:"decks"`
	LoginBonuses   []*UserLoginBonus                `json:"loginBonuses"`
	PresentHistory []*UserPresentAllReceivedHistory `json:"presentHistory"`
	Token          *UserOneTimeToken                `json:"token,omitempty"`
	Session        *Session                         `json:"session,omitempty"`
}

type stateInventory struct {
	Cards []*UserCard `json:"cards"`
	Items []*UserItem `json:"items"`
}

type stateInbox struct {
	Dynamic  []*UserPresent  `json:"dynamic"`
	Received map[int64]int64 `json:"received"`
}

type userState struct {
	ID                  int64
	Revision            int64
	Core                stateCore
	Inventory           stateInventory
	Inbox               stateInbox
	base                *userState
	eventsSinceSnapshot int
	eventBytes          int
}

type stateCacheEntry struct {
	id    int64
	state *userState
	size  int
}

type seedCacheEntry struct {
	id   int64
	rows []*UserPresent
	size int
}

type stateStore struct {
	gate          sync.RWMutex
	locks         [4096]sync.Mutex
	mu            sync.Mutex
	lru           *list.List
	items         map[int64]*list.Element
	bytes         int
	seedLRU       *list.List
	seeds         map[int64]*list.Element
	seedBytes     int
	sessionOwners map[string]int64
}

func newStateStore() *stateStore {
	return &stateStore{lru: list.New(), items: make(map[int64]*list.Element), seedLRU: list.New(), seeds: make(map[int64]*list.Element), sessionOwners: make(map[string]int64)}
}

func (s *stateStore) lock(id int64) func() {
	m := &s.locks[uint64(id)%uint64(len(s.locks))]
	m.Lock()
	return m.Unlock
}

func (s *stateStore) clear() {
	s.mu.Lock()
	s.lru.Init()
	s.items = make(map[int64]*list.Element)
	s.bytes = 0
	s.seedLRU.Init()
	s.seeds = make(map[int64]*list.Element)
	s.seedBytes = 0
	s.sessionOwners = make(map[string]int64)
	s.mu.Unlock()
}

func (s *stateStore) get(id int64) (*userState, bool) {
	s.mu.Lock()
	e, ok := s.items[id]
	if ok {
		s.lru.MoveToFront(e)
	}
	if !ok {
		s.mu.Unlock()
		return nil, false
	}
	committed := e.Value.(*stateCacheEntry).state
	s.mu.Unlock()
	copy := cloneUserState(committed)
	copy.base = committed
	return copy, true
}

// peek is only safe while holding the user's striped lock and without mutating the result.
func (s *stateStore) peek(id int64) (*userState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[id]
	if !ok {
		return nil, false
	}
	s.lru.MoveToFront(e)
	return e.Value.(*stateCacheEntry).state, true
}

func (h *Handler) loadUserStateRead(id int64) (*userState, error) {
	if st, ok := h.State.peek(id); ok {
		return st, nil
	}
	return h.loadUserState(id)
}

func (h *Handler) initializeState(c echo.Context) error {
	return h.initializeCluster(c)
}

func (h *Handler) resetLocal(ctx context.Context) error {
	h.State.gate.Lock()
	defer h.State.gate.Unlock()
	if err := initialize(ctx); err != nil {
		return err
	}
	h.State.clear()
	h.Masters.clear()
	return nil
}

func (s *stateStore) putOwned(st *userState) {
	size := estimateStateBytes(st)
	if size > stateCacheBytes {
		s.remove(st.ID)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[st.ID]; ok {
		entry := e.Value.(*stateCacheEntry)
		s.bytes -= entry.size
		entry.state, entry.size = st, size
		s.bytes += size
		s.lru.MoveToFront(e)
	} else {
		e := s.lru.PushFront(&stateCacheEntry{id: st.ID, state: st, size: size})
		s.items[st.ID] = e
		s.bytes += size
	}
	for (s.lru.Len() > stateCacheEntries || s.bytes > stateCacheBytes) && s.lru.Len() > 1 {
		last := s.lru.Back()
		entry := last.Value.(*stateCacheEntry)
		delete(s.items, entry.id)
		s.bytes -= entry.size
		s.lru.Remove(last)
	}
}

func (s *stateStore) remove(id int64) {
	s.mu.Lock()
	if e, ok := s.items[id]; ok {
		s.bytes -= e.Value.(*stateCacheEntry).size
		delete(s.items, id)
		s.lru.Remove(e)
	}
	s.mu.Unlock()
}

func cloneRows[T any](rows []*T) []*T {
	if rows == nil {
		return nil
	}
	out := make([]*T, len(rows))
	for i, row := range rows {
		value := *row
		out[i] = &value
	}
	return out
}

func cloneUserState(st *userState) *userState {
	copy := *st
	copy.base = nil
	if st.Core.User != nil {
		user := *st.Core.User
		copy.Core.User = &user
	}
	if st.Core.Token != nil {
		token := *st.Core.Token
		copy.Core.Token = &token
	}
	if st.Core.Session != nil {
		session := *st.Core.Session
		copy.Core.Session = &session
	}
	copy.Core.Devices = cloneRows(st.Core.Devices)
	copy.Core.Decks = cloneRows(st.Core.Decks)
	copy.Core.LoginBonuses = cloneRows(st.Core.LoginBonuses)
	copy.Core.PresentHistory = cloneRows(st.Core.PresentHistory)
	copy.Inventory.Cards = cloneRows(st.Inventory.Cards)
	copy.Inventory.Items = cloneRows(st.Inventory.Items)
	copy.Inbox.Dynamic = cloneRows(st.Inbox.Dynamic)
	if st.Inbox.Received != nil {
		copy.Inbox.Received = make(map[int64]int64, len(st.Inbox.Received))
		for id, at := range st.Inbox.Received {
			copy.Inbox.Received[id] = at
		}
	}
	return &copy
}

func estimateStateBytes(st *userState) int {
	size := 512 + 128*(len(st.Core.Devices)+len(st.Core.Decks)+len(st.Core.LoginBonuses)+len(st.Core.PresentHistory)+len(st.Inventory.Cards)+len(st.Inventory.Items)+len(st.Inbox.Dynamic)) + 24*len(st.Inbox.Received)
	for _, d := range st.Core.Devices {
		size += len(d.PlatformID)
	}
	for _, p := range st.Inbox.Dynamic {
		size += len(p.PresentMessage)
	}
	return size
}

func (s *stateStore) cachedSeed(id int64) ([]*UserPresent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.seeds[id]
	if !ok {
		return nil, false
	}
	s.seedLRU.MoveToFront(e)
	return e.Value.(*seedCacheEntry).rows, true
}

func (s *stateStore) putSeed(id int64, rows []*UserPresent) {
	size := 64
	for _, row := range rows {
		size += 128 + len(row.PresentMessage)
	}
	if size > seedCacheBytes {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.seeds[id]; ok {
		old := e.Value.(*seedCacheEntry)
		s.seedBytes -= old.size
		s.seedLRU.Remove(e)
	}
	e := s.seedLRU.PushFront(&seedCacheEntry{id: id, rows: rows, size: size})
	s.seeds[id] = e
	s.seedBytes += size
	for s.seedBytes > seedCacheBytes && s.seedLRU.Len() > 1 {
		last := s.seedLRU.Back()
		entry := last.Value.(*seedCacheEntry)
		delete(s.seeds, entry.id)
		s.seedBytes -= entry.size
		s.seedLRU.Remove(last)
	}
}

func (s *stateStore) sessionOwner(id string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, ok := s.sessionOwners[id]
	return owner, ok
}

func (s *stateStore) rememberSession(id string, owner int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sessionOwners) >= sessionCacheEntries {
		for key := range s.sessionOwners {
			delete(s.sessionOwners, key)
			break
		}
	}
	s.sessionOwners[id] = owner
}

func (s *stateStore) forgetSession(id string) {
	s.mu.Lock()
	delete(s.sessionOwners, id)
	s.mu.Unlock()
}

func (s *stateStore) replaceSession(old, next *Session, owner int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old != nil {
		delete(s.sessionOwners, old.SessionID)
	}
	if next != nil {
		if len(s.sessionOwners) >= sessionCacheEntries {
			for key := range s.sessionOwners {
				delete(s.sessionOwners, key)
				break
			}
		}
		s.sessionOwners[next.SessionID] = owner
	}
}

func (h *Handler) loadUserState(id int64) (*userState, error) {
	if st, ok := h.State.get(id); ok {
		return st, nil
	}
	st := &userState{ID: id}
	var corePayload, inventoryPayload, inboxPayload []byte
	err := h.DB.QueryRowx("SELECT c.revision,c.payload,i.payload,b.payload FROM user_state_core c JOIN user_state_inventory i USING(user_id) JOIN user_state_inbox b USING(user_id) WHERE c.user_id=?", id).Scan(&st.Revision, &corePayload, &inventoryPayload, &inboxPayload)
	exists := true
	switch err {
	case nil:
		if err := json.Unmarshal(corePayload, &st.Core); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(inventoryPayload, &st.Inventory); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(inboxPayload, &st.Inbox); err != nil {
			return nil, err
		}
	case sql.ErrNoRows:
		st.Revision = 0
		st.Core.User = new(User)
		if err := h.DB.Get(st.Core.User, "SELECT * FROM users WHERE id=?", id); err != nil {
			if err != sql.ErrNoRows {
				return nil, err
			}
			exists = false
		}
		if !exists {
			st.Core.User = nil
			break
		}
		if err := h.DB.Get(&st.Core.Banned, "SELECT EXISTS(SELECT 1 FROM user_bans WHERE user_id=?)", id); err != nil {
			return nil, err
		}
		if err := h.DB.Select(&st.Core.Devices, "SELECT * FROM user_devices WHERE user_id=? ORDER BY id", id); err != nil {
			return nil, err
		}
		if err := h.DB.Select(&st.Core.Decks, "SELECT * FROM user_decks WHERE user_id=? ORDER BY id", id); err != nil {
			return nil, err
		}
		if err := h.DB.Select(&st.Core.LoginBonuses, "SELECT * FROM user_login_bonuses WHERE user_id=? ORDER BY id", id); err != nil {
			return nil, err
		}
		if err := h.DB.Select(&st.Core.PresentHistory, "SELECT * FROM user_present_all_received_history WHERE user_id=? ORDER BY id", id); err != nil {
			return nil, err
		}
		if err := h.DB.Select(&st.Inventory.Cards, "SELECT * FROM user_cards WHERE user_id=? ORDER BY id", id); err != nil {
			return nil, err
		}
		if err := h.DB.Select(&st.Inventory.Items, "SELECT * FROM user_items WHERE user_id=? ORDER BY id", id); err != nil {
			return nil, err
		}
		if err := h.DB.Select(&st.Inbox.Dynamic, "SELECT * FROM user_presents WHERE user_id=? AND id>100000000000 ORDER BY id", id); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	rows, err := h.DB.Queryx("SELECT seq,payload FROM user_state_events WHERE user_id=? AND seq>? ORDER BY seq", id, st.Revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var payload []byte
		if err := rows.Scan(&seq, &payload); err != nil {
			return nil, err
		}
		if seq != st.Revision+1 {
			return nil, fmt.Errorf("state event gap: user=%d expected=%d got=%d", id, st.Revision+1, seq)
		}
		var delta stateDelta
		if err := json.Unmarshal(payload, &delta); err != nil {
			return nil, err
		}
		if !exists && delta.Create == nil {
			return nil, fmt.Errorf("missing create event: user=%d", id)
		}
		if err := applyStateDelta(st, delta); err != nil {
			return nil, err
		}
		st.Revision = seq
		st.eventsSinceSnapshot++
		st.eventBytes += len(payload)
		exists = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrUserNotFound
	}
	if st.Inbox.Received == nil {
		st.Inbox.Received = make(map[int64]int64)
	}
	sortDynamicPresents(st.Inbox.Dynamic)
	h.State.putOwned(st)
	copy := cloneUserState(st)
	copy.base = st
	return copy, nil
}

func (h *Handler) saveUserState(st *userState, core, inventory, inbox bool) (err error) {
	defer func() {
		if err != nil {
			h.State.remove(st.ID)
		}
	}()
	if st.base != nil && st.base.Revision != st.Revision {
		return fmt.Errorf("state revision conflict: user=%d", st.ID)
	}
	sortDynamicPresents(st.Inbox.Dynamic)
	req, err := newEventAppend(st, core, inventory, inbox)
	if err != nil {
		return err
	}
	if err := h.Writer.append(req); err != nil {
		return err
	}
	st.Revision++
	committed := cloneUserState(st)
	if st.base != nil {
		committed.eventsSinceSnapshot = st.base.eventsSinceSnapshot + 1
		committed.eventBytes = st.base.eventBytes + len(req.payload)
	} else {
		committed.eventsSinceSnapshot, committed.eventBytes = 1, len(req.payload)
	}
	h.State.putOwned(committed)
	h.State.replaceSession(req.oldSession, req.newSession, st.ID)
	st.base = committed
	if committed.eventsSinceSnapshot >= stateSnapshotEvents || committed.eventBytes >= stateSnapshotBytes {
		if compactErr := h.compactUserState(committed); compactErr != nil {
			log.Printf("state snapshot user=%d: %v", st.ID, compactErr)
		} else {
			committed.eventsSinceSnapshot, committed.eventBytes = 0, 0
		}
	}
	return nil
}

func (h *Handler) compactUserState(st *userState) error {
	corePayload, err := json.Marshal(st.Core)
	if err != nil {
		return err
	}
	inventoryPayload, err := json.Marshal(st.Inventory)
	if err != nil {
		return err
	}
	inboxPayload, err := json.Marshal(st.Inbox)
	if err != nil {
		return err
	}
	tx, err := h.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec("INSERT INTO user_state_core(user_id,revision,payload) VALUES (?,?,?) ON DUPLICATE KEY UPDATE revision=VALUES(revision),payload=VALUES(payload)", st.ID, st.Revision, corePayload); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO user_state_inventory(user_id,payload) VALUES (?,?) ON DUPLICATE KEY UPDATE payload=VALUES(payload)", st.ID, inventoryPayload); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO user_state_inbox(user_id,payload) VALUES (?,?) ON DUPLICATE KEY UPDATE payload=VALUES(payload)", st.ID, inboxPayload); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM user_state_events WHERE user_id=? AND seq<=?", st.ID, st.Revision); err != nil {
		return err
	}
	return tx.Commit()
}

func (st *userState) activeDeck() *UserDeck {
	for i := len(st.Core.Decks) - 1; i >= 0; i-- {
		if st.Core.Decks[i].DeletedAt == nil {
			return st.Core.Decks[i]
		}
	}
	return nil
}

func (st *userState) card(id int64) *UserCard {
	for _, c := range st.Inventory.Cards {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (st *userState) item(id int64) *UserItem {
	for _, v := range st.Inventory.Items {
		if v.ID == id {
			return v
		}
	}
	return nil
}

func (st *userState) validViewer(viewerID string) bool {
	for _, d := range st.Core.Devices {
		if d.PlatformID == viewerID {
			return true
		}
	}
	return false
}

func (st *userState) deckCards() []*UserCard {
	deck := st.activeDeck()
	if deck == nil {
		return nil
	}
	ids := []int64{deck.CardID1, deck.CardID2, deck.CardID3}
	cards := make([]*UserCard, 0, 3)
	for _, c := range st.Inventory.Cards {
		for _, id := range ids {
			if c.ID == id {
				cards = append(cards, c)
				break
			}
		}
	}
	return cards
}
