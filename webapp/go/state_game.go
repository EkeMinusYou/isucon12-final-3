package main

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
)

func (h *Handler) stateListGacha(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	masterData := requestMaster(c)
	masters := masterData.activeGachas(at)
	if len(masters) == 0 {
		return successResponse(c, &ListGachaResponse{Gachas: []*GachaData{}})
	}
	data := make([]*GachaData, 0, len(masters))
	for _, master := range masters {
		items := masterData.GachaItems[master.ID]
		if len(items) == 0 {
			return errorResponse(c, http.StatusNotFound, fmt.Errorf("not found gacha item"))
		}
		data = append(data, &GachaData{Gacha: master, GachaItem: items})
	}
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	token, err := h.issueStateToken(st, 1, at)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &ListGachaResponse{OneTimeToken: token, Gachas: data})
}

func (h *Handler) stateDrawGacha(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	gachaID := c.Param("gachaID")
	if gachaID == "" {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid gachaID"))
	}
	count, err := strconv.ParseInt(c.Param("n"), 10, 64)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	if count != 1 && count != 10 {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid draw gacha times"))
	}
	defer c.Request().Body.Close()
	req := new(DrawGachaRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	if err := h.consumeStateToken(st, req.OneTimeToken, 1, at); err != nil {
		if err == ErrInvalidToken {
			return errorResponse(c, http.StatusBadRequest, err)
		}
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	if err := stateViewerError(c, st, req.ViewerID); err != nil {
		return err
	}
	coins := count * 1000
	if st.Core.User.IsuCoin < coins {
		return errorResponse(c, http.StatusConflict, fmt.Errorf("not enough isucon"))
	}
	masterData := requestMaster(c)
	gachaNumber, _ := strconv.ParseInt(gachaID, 10, 64)
	master := masterData.gacha(gachaNumber, at)
	if master == nil {
		return errorResponse(c, http.StatusNotFound, fmt.Errorf("not found gacha"))
	}
	items := masterData.GachaItems[gachaNumber]
	if len(items) == 0 {
		return errorResponse(c, http.StatusNotFound, fmt.Errorf("not found gacha item"))
	}
	var total int64
	for _, item := range items {
		total += int64(item.Weight)
	}
	if total <= 0 {
		return errorResponse(c, http.StatusInternalServerError, fmt.Errorf("invalid gacha weights"))
	}
	presents := make([]*UserPresent, 0, count)
	for i := int64(0); i < count; i++ {
		r := rand.Int63n(total)
		var selected *GachaItemMaster
		for _, item := range items {
			r -= int64(item.Weight)
			if r < 0 {
				selected = item
				break
			}
		}
		if selected == nil {
			return errorResponse(c, http.StatusInternalServerError, fmt.Errorf("invalid gacha weights"))
		}
		pid, err := h.generateID()
		if err != nil {
			return errorResponse(c, http.StatusInternalServerError, err)
		}
		presents = append(presents, &UserPresent{ID: pid, UserID: id, SentAt: at, ItemType: selected.ItemType,
			ItemID: selected.ItemID, Amount: selected.Amount, PresentMessage: fmt.Sprintf("%sの付与アイテムです", master.Name),
			CreatedAt: at, UpdatedAt: at})
	}
	st.editUser().IsuCoin -= coins
	for _, present := range presents {
		st.addDynamic(present)
	}
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &DrawGachaResponse{Presents: presents})
}

func (h *Handler) stateListItem(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	token, err := h.issueStateToken(st, 2, at)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	items := st.Inventory.Items
	cards := st.Inventory.Cards
	if items == nil {
		items = []*UserItem{}
	}
	if cards == nil {
		cards = []*UserCard{}
	}
	return successResponse(c, &ListItemResponse{OneTimeToken: token, Items: items, User: st.Core.User, Cards: cards})
}

func (h *Handler) stateHome(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserStateRead(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	total := 0
	for _, card := range st.deckCards() {
		total += card.AmountPerSec
	}
	return successResponse(c, &HomeResponse{Now: at, User: st.Core.User, Deck: st.activeDeck(),
		TotalAmountPerSec: total, PastTime: at - st.Core.User.LastGetRewardAt})
}

func (h *Handler) stateReward(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	defer c.Request().Body.Close()
	req := new(RewardRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
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
	if st.activeDeck() == nil {
		return errorResponse(c, http.StatusNotFound, sql.ErrNoRows)
	}
	cards := st.deckCards()
	if len(cards) != 3 {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid cards length"))
	}
	total := 0
	for _, card := range cards {
		total += card.AmountPerSec
	}
	st.editUser().IsuCoin += int64(int(at-st.Core.User.LastGetRewardAt) * total)
	st.Core.User.LastGetRewardAt = at
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &RewardResponse{UpdatedResources: makeUpdatedResources(at, st.Core.User, nil, nil, nil, nil, nil, nil)})
}

func (h *Handler) stateUpdateDeck(c echo.Context) error {
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	defer c.Request().Body.Close()
	req := new(UpdateDeckRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	if len(req.CardIDs) != DeckCardNumber {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid number of cards"))
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
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
	seen := make(map[int64]bool, 3)
	for _, cid := range req.CardIDs {
		if seen[cid] || st.card(cid) == nil {
			return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid card ids"))
		}
		seen[cid] = true
	}
	if old := st.activeDeck(); old != nil {
		old = st.editDeck(old.ID)
		old.UpdatedAt = at
		old.DeletedAt = &at
	}
	newID, err := h.generateID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	deck := &UserDeck{ID: newID, UserID: id, CardID1: req.CardIDs[0], CardID2: req.CardIDs[1],
		CardID3: req.CardIDs[2], CreatedAt: at, UpdatedAt: at}
	st.addDeck(deck)
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &UpdateDeckResponse{UpdatedResources: makeUpdatedResources(at, nil, nil, nil, []*UserDeck{deck}, nil, nil, nil)})
}

func (h *Handler) stateAddExpToCard(c echo.Context) error {
	cardID, err := strconv.ParseInt(c.Param("cardID"), 10, 64)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	id, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	defer c.Request().Body.Close()
	req := new(AddExpToCardRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	unlock := h.lockUser(c, id)
	defer unlock()
	st, err := h.loadUserState(id)
	if err != nil {
		return stateNotFound(c, err)
	}
	if err := h.consumeStateToken(st, req.OneTimeToken, 2, at); err != nil {
		if err == ErrInvalidToken {
			return errorResponse(c, http.StatusBadRequest, err)
		}
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	if err := stateViewerError(c, st, req.ViewerID); err != nil {
		return err
	}
	card := st.card(cardID)
	if card == nil {
		return errorResponse(c, http.StatusNotFound, sql.ErrNoRows)
	}
	masterData := requestMaster(c)
	master := masterData.Items[card.CardID]
	if master == nil {
		return errorResponse(c, http.StatusNotFound, sql.ErrNoRows)
	}
	if card.Level == *master.MaxLevel {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("target card is max level"))
	}
	card = st.editCard(cardID)
	type consumption struct {
		item    *UserItem
		amount  int
		initial int
	}
	consumptions := make([]consumption, 0, len(req.Items))
	for _, v := range req.Items {
		item := st.item(v.ID)
		if item == nil || item.ItemType != 3 {
			return errorResponse(c, http.StatusNotFound, sql.ErrNoRows)
		}
		itemMaster := masterData.Items[item.ItemID]
		if itemMaster == nil {
			return errorResponse(c, http.StatusNotFound, sql.ErrNoRows)
		}
		if v.Amount > item.Amount {
			return errorResponse(c, http.StatusBadRequest, fmt.Errorf("item not enough"))
		}
		card.TotalExp += int64(*itemMaster.GainedExp * v.Amount)
		consumptions = append(consumptions, consumption{item: item, amount: v.Amount, initial: item.Amount})
	}
	for {
		threshold := int64(float64(*master.BaseExpPerLevel) * math.Pow(1.2, float64(card.Level-1)))
		if threshold > card.TotalExp {
			break
		}
		card.Level++
		card.AmountPerSec += (*master.MaxAmountPerSec - *master.AmountPerSec) / (*master.MaxLevel - 1)
	}
	card.UpdatedAt = at
	consumed := make([]*UserItem, 0, len(consumptions))
	for _, v := range consumptions {
		item := st.editItem(v.item.ID)
		item.Amount = v.initial - v.amount
		item.UpdatedAt = at
		result := *item
		consumed = append(consumed, &result)
	}
	if err := h.saveUserState(st); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &AddExpToCardResponse{UpdatedResources: makeUpdatedResources(at, nil, nil, []*UserCard{card}, nil, consumed, nil, nil)})
}
