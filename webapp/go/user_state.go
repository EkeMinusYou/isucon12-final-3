package main

import (
	"container/list"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

const stateCacheEntries = 256

type stateCore struct {
	User           *User                            `json:"user"`
	Banned         bool                             `json:"banned"`
	Devices        []*UserDevice                    `json:"devices"`
	Decks          []*UserDeck                      `json:"decks"`
	LoginBonuses   []*UserLoginBonus                `json:"loginBonuses"`
	PresentHistory []*UserPresentAllReceivedHistory `json:"presentHistory"`
	Token          *UserOneTimeToken                `json:"token,omitempty"`
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
	ID        int64
	Revision  int64
	Core      stateCore
	Inventory stateInventory
	Inbox     stateInbox
}

type stateCacheEntry struct {
	id    int64
	state *userState
}

type stateStore struct {
	gate  sync.RWMutex
	locks [4096]sync.Mutex
	mu    sync.Mutex
	lru   *list.List
	items map[int64]*list.Element
}

func newStateStore() *stateStore {
	return &stateStore{lru: list.New(), items: make(map[int64]*list.Element)}
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
	s.mu.Unlock()
}

func (s *stateStore) get(id int64) (*userState, bool, error) {
	s.mu.Lock()
	e, ok := s.items[id]
	if ok {
		s.lru.MoveToFront(e)
	}
	if !ok {
		s.mu.Unlock()
		return nil, false, nil
	}
	raw, err := json.Marshal(e.Value.(*stateCacheEntry).state)
	s.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	var copy userState
	if err := json.Unmarshal(raw, &copy); err != nil {
		return nil, false, err
	}
	return &copy, true, nil
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
	h.State.gate.Lock()
	defer h.State.gate.Unlock()
	if err := initialize(c); err != nil {
		return err
	}
	h.State.clear()
	h.Masters.clear()
	return nil
}

func (s *stateStore) put(st *userState) {
	raw, err := json.Marshal(st)
	if err != nil {
		s.remove(st.ID)
		return
	}
	var snapshot userState
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		s.remove(st.ID)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[st.ID]; ok {
		e.Value.(*stateCacheEntry).state = &snapshot
		s.lru.MoveToFront(e)
		return
	}
	e := s.lru.PushFront(&stateCacheEntry{id: st.ID, state: &snapshot})
	s.items[st.ID] = e
	if s.lru.Len() > stateCacheEntries {
		last := s.lru.Back()
		delete(s.items, last.Value.(*stateCacheEntry).id)
		s.lru.Remove(last)
	}
}

func (s *stateStore) remove(id int64) {
	s.mu.Lock()
	if e, ok := s.items[id]; ok {
		delete(s.items, id)
		s.lru.Remove(e)
	}
	s.mu.Unlock()
}

func (h *Handler) loadUserState(id int64) (*userState, error) {
	if st, ok, err := h.State.get(id); ok || err != nil {
		return st, err
	}
	st := &userState{ID: id}
	var payload []byte
	err := h.DB.QueryRowx("SELECT revision, payload FROM user_state_core WHERE user_id=?", id).Scan(&st.Revision, &payload)
	switch err {
	case nil:
		if err := json.Unmarshal(payload, &st.Core); err != nil {
			return nil, err
		}
		if err := h.DB.Get(&payload, "SELECT payload FROM user_state_inventory WHERE user_id=?", id); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &st.Inventory); err != nil {
			return nil, err
		}
		if err := h.DB.Get(&payload, "SELECT payload FROM user_state_inbox WHERE user_id=?", id); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &st.Inbox); err != nil {
			return nil, err
		}
	case sql.ErrNoRows:
		st.Revision = 0
		st.Core.User = new(User)
		if err := h.DB.Get(st.Core.User, "SELECT * FROM users WHERE id=?", id); err != nil {
			if err == sql.ErrNoRows {
				return nil, ErrUserNotFound
			}
			return nil, err
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
	if st.Inbox.Received == nil {
		st.Inbox.Received = make(map[int64]int64)
	}
	h.State.put(st)
	copy, _, err := h.State.get(id)
	return copy, err
}

func (h *Handler) saveUserState(st *userState, core, inventory, inbox bool, extra func(*sqlx.Tx) error) (err error) {
	defer func() {
		if err != nil {
			h.State.remove(st.ID)
		}
	}()
	if st.Revision == 0 {
		core, inventory, inbox = true, true, true
	}
	tx, err := h.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if extra != nil {
		if err := extra(tx); err != nil {
			return err
		}
	}
	if core || inventory || inbox {
		coreJSON, err := json.Marshal(st.Core)
		if err != nil {
			return err
		}
		if st.Revision == 0 {
			_, err = tx.Exec("INSERT INTO user_state_core(user_id, revision, payload) VALUES (?, 1, ?)", st.ID, coreJSON)
		} else {
			var result sql.Result
			result, err = tx.Exec("UPDATE user_state_core SET revision=revision+1, payload=? WHERE user_id=? AND revision=?", coreJSON, st.ID, st.Revision)
			if err == nil {
				n, e := result.RowsAffected()
				if e != nil {
					return e
				}
				if n != 1 {
					return fmt.Errorf("state revision conflict: user=%d", st.ID)
				}
			}
		}
		if err != nil {
			return err
		}
		if inventory {
			b, err := json.Marshal(st.Inventory)
			if err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO user_state_inventory(user_id,payload) VALUES (?,?) ON DUPLICATE KEY UPDATE payload=VALUES(payload)", st.ID, b); err != nil {
				return err
			}
		}
		if inbox {
			b, err := json.Marshal(st.Inbox)
			if err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO user_state_inbox(user_id,payload) VALUES (?,?) ON DUPLICATE KEY UPDATE payload=VALUES(payload)", st.ID, b); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if core || inventory || inbox {
		st.Revision++
		h.State.put(st)
	}
	return nil
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
