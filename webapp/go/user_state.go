package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
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
	ID        int64
	Revision  int64
	Core      stateCore
	Inventory stateInventory
	Inbox     stateInbox
	base      *userState
	changes   stateChanges
}

type stateStore struct {
	gate          sync.RWMutex
	locks         [4096]sync.Mutex
	mu            sync.RWMutex
	items         map[int64]*userState
	seeds         map[int64][]*UserPresent
	sessionOwners map[string]int64
}

func newStateStore() *stateStore {
	return &stateStore{items: make(map[int64]*userState), sessionOwners: make(map[string]int64)}
}

func (s *stateStore) lock(id int64) func() {
	m := &s.locks[uint64(id)%uint64(len(s.locks))]
	m.Lock()
	return m.Unlock
}

func (s *stateStore) replace(states map[int64]*userState, seeds map[int64][]*UserPresent) {
	s.mu.Lock()
	s.items = states
	s.seeds = seeds
	s.sessionOwners = make(map[string]int64)
	for id, st := range states {
		if st.Core.Session != nil {
			s.sessionOwners[st.Core.Session.SessionID] = id
		}
	}
	s.mu.Unlock()
}

func (s *stateStore) get(id int64) (*userState, bool) {
	s.mu.RLock()
	committed, ok := s.items[id]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return newWorkingState(committed), true
}

// peek is only safe while holding the user's striped lock and without mutating the result.
func (s *stateStore) peek(id int64) (*userState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.items[id]
	return st, ok
}

func (h *Handler) loadUserStateRead(id int64) (*userState, error) {
	if st, ok := h.State.peek(id); ok {
		return st, nil
	}
	return nil, ErrUserNotFound
}

func (h *Handler) initializeState(c echo.Context) error {
	return h.initializeCluster(c)
}

func (h *Handler) resetLocal(ctx context.Context) error {
	h.CheckpointMu.Lock()
	defer h.CheckpointMu.Unlock()
	h.State.gate.Lock()
	defer h.State.gate.Unlock()
	if err := initialize(ctx); err != nil {
		return err
	}
	seeds, err := h.loadSeedPresents()
	if err != nil {
		return err
	}
	states, err := h.loadInitialStates(ctx)
	if err != nil {
		return err
	}
	if err := h.Writer.reset(states, seeds); err != nil {
		return err
	}
	h.State.replace(states, seeds)
	h.LastReset = time.Now()
	if h.CheckpointReset != nil {
		select {
		case h.CheckpointReset <- struct{}{}:
		default:
		}
	}
	if snapshot, err := os.Stat(snapshotPath(h.Writer.dir, 0)); err == nil {
		if seedFile, seedErr := os.Stat(seedsPath(h.Writer.dir)); seedErr == nil {
			log.Printf("initialized local state: users=%d snapshot_bytes=%d seed_bytes=%d", len(states), snapshot.Size(), seedFile.Size())
		}
	}
	h.Masters.clear()
	debug.FreeOSMemory()
	return nil
}

func (s *stateStore) putOwned(st *userState) {
	s.mu.Lock()
	s.items[st.ID] = st
	s.mu.Unlock()
}

func (s *stateStore) snapshot() map[int64]*userState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	states := make(map[int64]*userState, len(s.items))
	for id, st := range s.items {
		states[id] = st
	}
	return states
}

func (s *stateStore) seedRows(id int64) []*UserPresent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seeds[id]
}

func (h *Handler) loadSeedPresents() (map[int64][]*UserPresent, error) {
	seeds := make(map[int64][]*UserPresent)
	if h.Cluster.Self == 0 {
		return seeds, nil
	}
	rows, err := h.DB.Queryx(`SELECT * FROM user_presents WHERE id<=? AND user_id>0 AND
		((user_id-1) % 5 = ? OR ((user_id-1) % 5 = 0 AND ((user_id-1) DIV 5) % 4 = ?))`,
		seedPresentMaxID, h.Cluster.Self, h.Cluster.Self-1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		present := new(UserPresent)
		if err := rows.StructScan(present); err != nil {
			return nil, err
		}
		if present.UserID > 0 && h.Cluster.owner(present.UserID) == h.Cluster.Self {
			seeds[present.UserID] = append(seeds[present.UserID], present)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, presents := range seeds {
		sort.Slice(presents, func(i, j int) bool {
			if presents[i].CreatedAt != presents[j].CreatedAt {
				return presents[i].CreatedAt > presents[j].CreatedAt
			}
			return presents[i].ID < presents[j].ID
		})
	}
	return seeds, nil
}

func (s *stateStore) sessionOwner(id string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	owner, ok := s.sessionOwners[id]
	return owner, ok
}

func (s *stateStore) replaceSession(old, next *Session, owner int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old != nil {
		delete(s.sessionOwners, old.SessionID)
	}
	if next != nil {
		s.sessionOwners[next.SessionID] = owner
	}
}

func (h *Handler) loadUserState(id int64) (*userState, error) {
	if st, ok := h.State.get(id); ok {
		return st, nil
	}
	return nil, ErrUserNotFound
}

func (h *Handler) saveUserState(st *userState) (err error) {
	if st.base != nil && st.base.Revision != st.Revision {
		return fmt.Errorf("state revision conflict: user=%d", st.ID)
	}
	if st.changes.dynamicOwned {
		sortDynamicPresents(st.Inbox.Dynamic)
	}
	req, err := newEventAppend(st)
	if err != nil {
		return err
	}
	if err := h.Writer.append(req); err != nil {
		return err
	}
	committed := st.promoteCommitted()
	h.State.putOwned(committed)
	h.State.replaceSession(req.oldSession, req.newSession, st.ID)
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
