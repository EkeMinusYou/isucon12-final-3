package main

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (h *Handler) stateCreateUser(c echo.Context) error {
	defer c.Request().Body.Close()
	req := new(CreateUserRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	if req.ViewerID == "" || req.PlatformType < 1 || req.PlatformType > 3 {
		return errorResponse(c, http.StatusBadRequest, ErrInvalidRequestBody)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, ErrGetRequestTime)
	}
	userID, err := h.generateID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	unlock := h.lockUser(c, userID)
	defer unlock()
	st := &userState{ID: userID}
	st.Inbox.Received = make(map[int64]int64)
	st.Core.User = &User{ID: userID, LastGetRewardAt: at, LastActivatedAt: at,
		RegisteredAt: at, CreatedAt: at, UpdatedAt: at}
	deviceID, err := h.generateID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	device := &UserDevice{ID: deviceID, UserID: userID, PlatformID: req.ViewerID,
		PlatformType: req.PlatformType, CreatedAt: at, UpdatedAt: at}
	st.Core.Devices = []*UserDevice{device}
	masters := requestMaster(c)
	item := masters.Items[2]
	if item == nil {
		return errorResponse(c, http.StatusNotFound, ErrItemNotFound)
	}
	cards := make([]*UserCard, 0, 3)
	for i := 0; i < 3; i++ {
		cardID, err := h.generateID()
		if err != nil {
			return errorResponse(c, http.StatusInternalServerError, err)
		}
		cards = append(cards, &UserCard{ID: cardID, UserID: userID, CardID: item.ID,
			AmountPerSec: *item.AmountPerSec, Level: 1, CreatedAt: at, UpdatedAt: at})
	}
	st.Inventory.Cards = cards
	deckID, err := h.generateID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	deck := &UserDeck{ID: deckID, UserID: userID, CardID1: cards[0].ID, CardID2: cards[1].ID,
		CardID3: cards[2].ID, CreatedAt: at, UpdatedAt: at}
	st.Core.Decks = []*UserDeck{deck}
	bonuses, presents, err := h.stateLoginRewards(st, masters, at)
	if err != nil {
		return stateGrantError(c, err)
	}
	sessionID, err := h.generateID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	sessionToken, err := generateUUID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	st.Core.Session = &Session{ID: sessionID, UserID: userID, SessionID: sessionToken, CreatedAt: at, UpdatedAt: at, ExpiredAt: at + 86400}
	if err := h.saveUserState(st, true, true, true); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &CreateUserResponse{UserID: userID, ViewerID: req.ViewerID, SessionID: sessionToken,
		CreatedAt: at, UpdatedResources: makeUpdatedResources(at, st.Core.User, device, cards,
			[]*UserDeck{deck}, nil, bonuses, presents)})
}
