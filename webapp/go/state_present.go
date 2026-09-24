package main

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

const seedPresentMaxID int64 = 100000000000

func (h *Handler) seedPresents(userID int64) ([]*UserPresent, error) {
	result := make([]*UserPresent, 0)
	err := h.DB.Select(&result, "SELECT * FROM user_presents WHERE user_id=? AND id<=? ORDER BY id", userID, seedPresentMaxID)
	return result, err
}

func combinedPresents(seed []*UserPresent, st *userState, includeReceived bool) []*UserPresent {
	presents := make([]*UserPresent, 0, len(seed)+len(st.Inbox.Dynamic))
	for _, original := range seed {
		p := *original
		if receivedAt, ok := st.Inbox.Received[p.ID]; ok {
			p.UpdatedAt = receivedAt
			p.DeletedAt = &receivedAt
		}
		if includeReceived || p.DeletedAt == nil {
			presents = append(presents, &p)
		}
	}
	for _, p := range st.Inbox.Dynamic {
		if includeReceived || p.DeletedAt == nil {
			presents = append(presents, p)
		}
	}
	sort.Slice(presents, func(i, j int) bool {
		if presents[i].CreatedAt != presents[j].CreatedAt {
			return presents[i].CreatedAt > presents[j].CreatedAt
		}
		return presents[i].ID < presents[j].ID
	})
	return presents
}

func (h *Handler) stateListPresent(c echo.Context) error {
	n, err := strconv.Atoi(c.Param("n"))
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid index number (n) parameter"))
	}
	if n <= 0 || n > int(^uint(0)>>1)/PresentCountPerPage {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("index number (n) should be more than or equal to 1"))
	}
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid userID parameter"))
	}
	unlock := h.State.lock(id)
	defer unlock()
	st, err := h.loadUserStateRead(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	seed, err := h.seedPresents(id)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	all := combinedPresents(seed, st, false)
	offset := PresentCountPerPage * (n - 1)
	if offset >= len(all) {
		return successResponse(c, &ListPresentResponse{Presents: []*UserPresent{}, IsNext: false})
	}
	end := offset + PresentCountPerPage
	if end > len(all) {
		end = len(all)
	}
	return successResponse(c, &ListPresentResponse{Presents: all[offset:end], IsNext: end < len(all)})
}

func (h *Handler) stateReceivePresent(c echo.Context) error {
	defer c.Request().Body.Close()
	req := new(ReceivePresentRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	if len(req.PresentIDs) == 0 {
		return errorResponse(c, http.StatusUnprocessableEntity, fmt.Errorf("presentIds is empty"))
	}
	unlock := h.State.lock(id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	if err := stateViewerError(c, st, req.ViewerID); err != nil {
		return err
	}
	seen := make(map[int64]bool, len(req.PresentIDs))
	seedIDs := make([]int64, 0, len(req.PresentIDs))
	dynamic := make(map[int64]*UserPresent, len(st.Inbox.Dynamic))
	for _, p := range st.Inbox.Dynamic {
		dynamic[p.ID] = p
	}
	for _, pid := range req.PresentIDs {
		if seen[pid] {
			continue
		}
		seen[pid] = true
		if pid <= seedPresentMaxID {
			seedIDs = append(seedIDs, pid)
		}
	}
	toReceive := make([]*UserPresent, 0, len(req.PresentIDs))
	if len(seedIDs) > 0 {
		query, params, err := sqlx.In("SELECT * FROM user_presents WHERE user_id=? AND id IN (?) AND deleted_at IS NULL", id, seedIDs)
		if err != nil {
			return errorResponse(c, http.StatusBadRequest, err)
		}
		seed := make([]*UserPresent, 0, len(seedIDs))
		if err := h.DB.Select(&seed, query, params...); err != nil {
			return errorResponse(c, http.StatusInternalServerError, err)
		}
		for _, p := range seed {
			if _, received := st.Inbox.Received[p.ID]; !received {
				toReceive = append(toReceive, p)
			}
		}
	}
	for _, pid := range req.PresentIDs {
		if p := dynamic[pid]; p != nil && p.DeletedAt == nil {
			toReceive = append(toReceive, p)
			delete(dynamic, pid)
		}
	}
	if len(toReceive) == 0 {
		return successResponse(c, &ReceivePresentResponse{UpdatedResources: makeUpdatedResources(at, nil, nil, nil, nil, nil, nil, []*UserPresent{})})
	}
	sort.Slice(toReceive, func(i, j int) bool { return toReceive[i].ID < toReceive[j].ID })
	for _, p := range toReceive {
		if p.ID <= seedPresentMaxID {
			st.Inbox.Received[p.ID] = at
		}
		p.UpdatedAt = at
		p.DeletedAt = &at
		if err := h.stateGrantItem(st, p.ItemID, p.ItemType, int64(p.Amount), at); err != nil {
			return stateGrantError(c, err)
		}
	}
	if err := h.saveUserState(st, true, true, true, nil); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &ReceivePresentResponse{UpdatedResources: makeUpdatedResources(at, nil, nil, nil, nil, nil, nil, toReceive)})
}

func (h *Handler) stateAdminUser(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	unlock := h.State.lock(id)
	defer unlock()
	st, err := h.loadUserStateRead(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	seed, err := h.seedPresents(id)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	presents := combinedPresents(seed, st, true)
	if presents == nil {
		presents = []*UserPresent{}
	}
	return successResponse(c, &AdminUserResponse{
		User: st.Core.User, UserDevices: emptyDevices(st.Core.Devices),
		UserCards: emptyCards(st.Inventory.Cards), UserDecks: emptyDecks(st.Core.Decks),
		UserItems: emptyItems(st.Inventory.Items), UserLoginBonuses: emptyBonuses(st.Core.LoginBonuses),
		UserPresents: presents, UserPresentAllReceivedHistory: emptyHistory(st.Core.PresentHistory),
	})
}

func emptyDevices(v []*UserDevice) []*UserDevice {
	if v == nil {
		return []*UserDevice{}
	}
	return v
}
func emptyCards(v []*UserCard) []*UserCard {
	if v == nil {
		return []*UserCard{}
	}
	return v
}
func emptyDecks(v []*UserDeck) []*UserDeck {
	if v == nil {
		return []*UserDeck{}
	}
	return v
}
func emptyItems(v []*UserItem) []*UserItem {
	if v == nil {
		return []*UserItem{}
	}
	return v
}
func emptyBonuses(v []*UserLoginBonus) []*UserLoginBonus {
	if v == nil {
		return []*UserLoginBonus{}
	}
	return v
}
func emptyHistory(v []*UserPresentAllReceivedHistory) []*UserPresentAllReceivedHistory {
	if v == nil {
		return []*UserPresentAllReceivedHistory{}
	}
	return v
}

func (h *Handler) stateAdminBanUser(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	unlock := h.State.lock(id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		if err == ErrUserNotFound {
			return errorResponse(c, http.StatusBadRequest, err)
		}
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	st.Core.Banned = true
	if err := h.saveUserState(st, true, false, false, nil); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &AdminBanUserResponse{User: st.Core.User})
}
