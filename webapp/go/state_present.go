package main

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/labstack/echo/v4"
)

const seedPresentMaxID int64 = 100000000000

func (h *Handler) seedPresents(userID int64) []*UserPresent {
	return h.State.seedRows(userID)
}

func combinedPresents(seed []*UserPresent, st *userState, includeReceived bool) []*UserPresent {
	return mergePresents(seed, st, includeReceived, 0)
}

func mergePresents(seed []*UserPresent, st *userState, includeReceived bool, limit int) []*UserPresent {
	presents := make([]*UserPresent, 0)
	i, j := 0, 0
	for (i < len(seed) || j < len(st.Inbox.Dynamic)) && (limit == 0 || len(presents) < limit) {
		useSeed := j >= len(st.Inbox.Dynamic)
		if !useSeed && i < len(seed) {
			a, b := seed[i], st.Inbox.Dynamic[j]
			useSeed = a.CreatedAt > b.CreatedAt || (a.CreatedAt == b.CreatedAt && a.ID < b.ID)
		}
		if useSeed {
			original := seed[i]
			i++
			p := *original
			if receivedAt, ok := st.Inbox.Received[p.ID]; ok {
				p.UpdatedAt, p.DeletedAt = receivedAt, &receivedAt
			}
			if includeReceived || p.DeletedAt == nil {
				presents = append(presents, &p)
			}
		} else {
			p := st.Inbox.Dynamic[j]
			j++
			if includeReceived || p.DeletedAt == nil {
				presents = append(presents, p)
			}
		}
	}
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
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserStateRead(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	seed := h.seedPresents(id)
	all := mergePresents(seed, st, false, n*PresentCountPerPage+1)
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
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	if err := stateViewerError(c, st, req.ViewerID); err != nil {
		return err
	}
	seen := make(map[int64]bool, len(req.PresentIDs))
	needSeed := false
	for _, pid := range req.PresentIDs {
		seen[pid] = true
		if pid <= seedPresentMaxID {
			needSeed = true
		}
	}
	toReceive := make([]*UserPresent, 0, len(seen))
	if needSeed {
		seed := h.seedPresents(id)
		for _, original := range seed {
			if !seen[original.ID] || original.DeletedAt != nil {
				continue
			}
			if _, received := st.Inbox.Received[original.ID]; received {
				continue
			}
			p := *original
			toReceive = append(toReceive, &p)
		}
	}
	for _, p := range st.Inbox.Dynamic {
		if seen[p.ID] && p.DeletedAt == nil {
			toReceive = append(toReceive, p)
			delete(seen, p.ID)
		}
	}
	if len(toReceive) == 0 {
		return successResponse(c, &ReceivePresentResponse{UpdatedResources: makeUpdatedResources(at, nil, nil, nil, nil, nil, nil, []*UserPresent{})})
	}
	sort.Slice(toReceive, func(i, j int) bool { return toReceive[i].ID < toReceive[j].ID })
	for i, p := range toReceive {
		if p.ID <= seedPresentMaxID {
			st.setReceived(p.ID, at)
		} else {
			p = st.editDynamic(p.ID)
			toReceive[i] = p
		}
		p.UpdatedAt = at
		p.DeletedAt = &at
		if err := h.stateGrantItem(st, requestMaster(c), p.ItemID, p.ItemType, int64(p.Amount), at); err != nil {
			return stateGrantError(c, err)
		}
	}
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &ReceivePresentResponse{UpdatedResources: makeUpdatedResources(at, nil, nil, nil, nil, nil, nil, toReceive)})
}

func (h *Handler) stateAdminUser(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserStateRead(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	seed := h.seedPresents(id)
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
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		if err == ErrUserNotFound {
			return errorResponse(c, http.StatusBadRequest, err)
		}
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	st.setBanned(true)
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &AdminBanUserResponse{User: st.Core.User})
}
