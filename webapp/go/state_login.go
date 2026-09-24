package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

func (h *Handler) stateGrantItem(st *userState, masters *masterSnapshot, itemID int64, itemType int, amount int64, at int64) error {
	if itemType == 1 {
		st.editUser().IsuCoin += amount
		return nil
	}
	switch itemType {
	case 2:
		item := masters.Items[itemID]
		if item == nil || item.ItemType != itemType {
			return ErrItemNotFound
		}
		id, err := h.generateID()
		if err != nil {
			return err
		}
		st.addCard(&UserCard{
			ID: id, UserID: st.ID, CardID: item.ID, AmountPerSec: *item.AmountPerSec,
			Level: 1, TotalExp: 0, CreatedAt: at, UpdatedAt: at,
		})
	case 3, 4:
		item := masters.Items[itemID]
		if item == nil || item.ItemType != itemType {
			return ErrItemNotFound
		}
		for _, v := range st.Inventory.Items {
			if v.ItemID == item.ID {
				v = st.editItem(v.ID)
				v.Amount += int(amount)
				v.UpdatedAt = at
				return nil
			}
		}
		id, err := h.generateID()
		if err != nil {
			return err
		}
		st.addItem(&UserItem{
			ID: id, UserID: st.ID, ItemType: item.ItemType, ItemID: item.ID,
			Amount: int(amount), CreatedAt: at, UpdatedAt: at,
		})
	default:
		return ErrInvalidItemType
	}
	return nil
}

func (h *Handler) stateLoginRewards(st *userState, masters *masterSnapshot, at int64) ([]*UserLoginBonus, []*UserPresent, error) {
	bonuses := masters.activeBonuses(at)
	sentBonuses := make([]*UserLoginBonus, 0)
	for _, bonus := range bonuses {
		var progress *UserLoginBonus
		for _, v := range st.Core.LoginBonuses {
			if v.LoginBonusID == bonus.ID {
				progress = v
				break
			}
		}
		if progress == nil {
			id, err := h.generateID()
			if err != nil {
				return nil, nil, err
			}
			progress = &UserLoginBonus{ID: id, UserID: st.ID, LoginBonusID: bonus.ID, LoopCount: 1, CreatedAt: at, UpdatedAt: at}
			st.addBonus(progress)
		}
		if progress.LastRewardSequence < bonus.ColumnCount {
			progress = st.editBonus(progress.ID)
			progress.LastRewardSequence++
		} else if bonus.Looped {
			progress = st.editBonus(progress.ID)
			progress.LoopCount++
			progress.LastRewardSequence = 1
		} else {
			continue
		}
		progress.UpdatedAt = at
		reward := masters.BonusRewards[bonus.ID][progress.LastRewardSequence]
		if reward == nil {
			return nil, nil, ErrLoginBonusRewardNotFound
		}
		if err := h.stateGrantItem(st, masters, reward.ItemID, reward.ItemType, reward.Amount, at); err != nil {
			return nil, nil, err
		}
		sentBonuses = append(sentBonuses, progress)
	}
	all := masters.activePresents(at)
	presents := make([]*UserPresent, 0)
	for _, master := range all {
		found := false
		for _, history := range st.Core.PresentHistory {
			if history.PresentAllID == master.ID {
				found = true
				break
			}
		}
		if found {
			continue
		}
		id, err := h.generateID()
		if err != nil {
			return nil, nil, err
		}
		p := &UserPresent{ID: id, UserID: st.ID, SentAt: at, ItemType: master.ItemType,
			ItemID: master.ItemID, Amount: int(master.Amount), PresentMessage: master.PresentMessage,
			CreatedAt: at, UpdatedAt: at}
		historyID, err := h.generateID()
		if err != nil {
			return nil, nil, err
		}
		history := &UserPresentAllReceivedHistory{ID: historyID, UserID: st.ID,
			PresentAllID: master.ID, ReceivedAt: at, CreatedAt: at, UpdatedAt: at}
		st.addPresentHistory(history)
		st.addDynamic(p)
		presents = append(presents, p)
	}
	st.editUser().LastActivatedAt = at
	st.Core.User.UpdatedAt = at
	return sentBonuses, presents, nil
}

func (h *Handler) stateLogin(c echo.Context) error {
	defer c.Request().Body.Close()
	req := new(LoginRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	at, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	unlock := h.lockUser(c, req.UserID)
	defer unlock()
	st, err := h.loadUserState(req.UserID)
	if err != nil {
		if err == ErrUserNotFound {
			return errorResponse(c, http.StatusNotFound, err)
		}
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	if st.Core.Banned {
		return errorResponse(c, http.StatusForbidden, ErrForbidden)
	}
	if !st.validViewer(req.ViewerID) {
		return errorResponse(c, http.StatusNotFound, ErrUserDeviceNotFound)
	}
	id, err := h.generateID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	sessID, err := generateUUID()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	var bonuses []*UserLoginBonus
	var presents []*UserPresent
	daily := !isCompleteTodayLogin(time.Unix(st.Core.User.LastActivatedAt, 0), time.Unix(at, 0))
	if daily {
		bonuses, presents, err = h.stateLoginRewards(st, requestMaster(c), at)
		if err != nil {
			if err == ErrItemNotFound || err == ErrLoginBonusRewardNotFound {
				return errorResponse(c, http.StatusNotFound, err)
			}
			if err == ErrInvalidItemType {
				return errorResponse(c, http.StatusBadRequest, err)
			}
			return errorResponse(c, http.StatusInternalServerError, err)
		}
	} else {
		st.editUser().LastActivatedAt = at
		st.Core.User.UpdatedAt = at
	}
	st.setSession(&Session{ID: id, UserID: req.UserID, SessionID: sessID, CreatedAt: at, UpdatedAt: at, ExpiredAt: at + 86400})
	err = h.saveUserState(st)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	return successResponse(c, &LoginResponse{ViewerID: req.ViewerID, SessionID: sessID,
		UpdatedResources: makeUpdatedResources(at, st.Core.User, nil, nil, nil, nil, bonuses, presents)})
}

func stateTokenValid(st *userState, token string, tokenType int, at int64) error {
	t := st.Core.Token
	if t == nil || t.Token != token || t.TokenType != tokenType || t.ExpiredAt < at {
		return ErrInvalidToken
	}
	st.setToken(nil)
	return nil
}

func (h *Handler) issueStateToken(st *userState, tokenType int, at int64) (string, error) {
	id, err := h.generateID()
	if err != nil {
		return "", err
	}
	value, err := generateUUID()
	if err != nil {
		return "", err
	}
	st.setToken(&UserOneTimeToken{ID: id, UserID: st.ID, Token: value, TokenType: tokenType,
		CreatedAt: at, UpdatedAt: at, ExpiredAt: at + 600})
	return value, nil
}

func (h *Handler) consumeStateToken(st *userState, token string, tokenType int, at int64) error {
	err := stateTokenValid(st, token, tokenType, at)
	if err == ErrInvalidToken && st.Core.Token != nil && st.Core.Token.Token == token && st.Core.Token.ExpiredAt < at {
		st.setToken(nil)
		if saveErr := h.saveUserState(st); saveErr != nil {
			return saveErr
		}
		return err
	}
	if err != nil {
		return err
	}
	return h.saveUserState(st)
}

func stateViewerError(c echo.Context, st *userState, viewerID string) error {
	if !st.validViewer(viewerID) {
		return errorResponse(c, http.StatusNotFound, ErrUserDeviceNotFound)
	}
	return nil
}

func stateNotFound(c echo.Context, err error) error {
	if err == ErrUserNotFound {
		return errorResponse(c, http.StatusNotFound, err)
	}
	return errorResponse(c, http.StatusInternalServerError, err)
}

func stateGrantError(c echo.Context, err error) error {
	switch err {
	case ErrUserNotFound, ErrItemNotFound, ErrLoginBonusRewardNotFound:
		return errorResponse(c, http.StatusNotFound, err)
	case ErrInvalidItemType:
		return errorResponse(c, http.StatusBadRequest, err)
	default:
		return errorResponse(c, http.StatusInternalServerError, fmt.Errorf("grant item: %w", err))
	}
}
