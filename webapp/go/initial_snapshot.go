package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

const initialSnapshotBatchSize = 128

// snapshotInitialUsers moves the SQL seed users into the normal durable state format.
func (h *Handler) snapshotInitialUsers(ctx context.Context) error {
	if h.Cluster.Self == 0 {
		return nil
	}
	started := time.Now()
	var users []*User
	if err := h.DB.SelectContext(ctx, &users, "SELECT * FROM users ORDER BY id"); err != nil {
		return fmt.Errorf("load initial users: %w", err)
	}
	owned := make([]*User, 0, len(users)/4+1)
	for _, user := range users {
		if h.Cluster.owner(user.ID) == h.Cluster.Self {
			owned = append(owned, user)
		}
	}
	for start := 0; start < len(owned); start += initialSnapshotBatchSize {
		end := start + initialSnapshotBatchSize
		if end > len(owned) {
			end = len(owned)
		}
		if err := h.snapshotInitialBatch(ctx, owned[start:end]); err != nil {
			return fmt.Errorf("snapshot initial users %d-%d: %w", owned[start].ID, owned[end-1].ID, err)
		}
	}
	log.Printf("initial snapshots host=%d users=%d elapsed=%s", h.Cluster.Self+1, len(owned), time.Since(started))
	return nil
}

func (h *Handler) snapshotInitialBatch(ctx context.Context, users []*User) error {
	ids := make([]int64, 0, len(users))
	states := make([]*userState, 0, len(users))
	byID := make(map[int64]*userState, len(users))
	for _, user := range users {
		st := &userState{ID: user.ID}
		st.Core.User = user
		st.Inbox.Received = make(map[int64]int64)
		ids = append(ids, user.ID)
		states = append(states, st)
		byID[user.ID] = st
	}

	var bans []int64
	if err := h.selectInitialBatch(ctx, &bans, "SELECT user_id FROM user_bans WHERE user_id IN (?)", ids); err != nil {
		return err
	}
	for _, id := range bans {
		byID[id].Core.Banned = true
	}
	var devices []*UserDevice
	if err := h.selectInitialBatch(ctx, &devices, "SELECT * FROM user_devices WHERE user_id IN (?) ORDER BY user_id,id", ids); err != nil {
		return err
	}
	for _, row := range devices {
		byID[row.UserID].Core.Devices = append(byID[row.UserID].Core.Devices, row)
	}
	var decks []*UserDeck
	if err := h.selectInitialBatch(ctx, &decks, "SELECT * FROM user_decks WHERE user_id IN (?) ORDER BY user_id,id", ids); err != nil {
		return err
	}
	for _, row := range decks {
		byID[row.UserID].Core.Decks = append(byID[row.UserID].Core.Decks, row)
	}
	var bonuses []*UserLoginBonus
	if err := h.selectInitialBatch(ctx, &bonuses, "SELECT * FROM user_login_bonuses WHERE user_id IN (?) ORDER BY user_id,id", ids); err != nil {
		return err
	}
	for _, row := range bonuses {
		byID[row.UserID].Core.LoginBonuses = append(byID[row.UserID].Core.LoginBonuses, row)
	}
	var history []*UserPresentAllReceivedHistory
	if err := h.selectInitialBatch(ctx, &history, "SELECT * FROM user_present_all_received_history WHERE user_id IN (?) ORDER BY user_id,id", ids); err != nil {
		return err
	}
	for _, row := range history {
		byID[row.UserID].Core.PresentHistory = append(byID[row.UserID].Core.PresentHistory, row)
	}
	var cards []*UserCard
	if err := h.selectInitialBatch(ctx, &cards, "SELECT * FROM user_cards WHERE user_id IN (?) ORDER BY user_id,id", ids); err != nil {
		return err
	}
	for _, row := range cards {
		byID[row.UserID].Inventory.Cards = append(byID[row.UserID].Inventory.Cards, row)
	}
	var items []*UserItem
	if err := h.selectInitialBatch(ctx, &items, "SELECT * FROM user_items WHERE user_id IN (?) ORDER BY user_id,id", ids); err != nil {
		return err
	}
	for _, row := range items {
		byID[row.UserID].Inventory.Items = append(byID[row.UserID].Inventory.Items, row)
	}
	return h.insertInitialSnapshots(ctx, states)
}

func (h *Handler) selectInitialBatch(ctx context.Context, dest interface{}, query string, ids []int64) error {
	bound, args, err := sqlx.In(query, ids)
	if err != nil {
		return err
	}
	if err := h.DB.SelectContext(ctx, dest, h.DB.Rebind(bound), args...); err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	return nil
}

func (h *Handler) insertInitialSnapshots(ctx context.Context, states []*userState) error {
	coreArgs := make([]interface{}, 0, 3*len(states))
	inventoryArgs := make([]interface{}, 0, 2*len(states))
	inboxArgs := make([]interface{}, 0, 2*len(states))
	for _, st := range states {
		core, err := json.Marshal(st.Core)
		if err != nil {
			return err
		}
		inventory, err := json.Marshal(st.Inventory)
		if err != nil {
			return err
		}
		inbox, err := json.Marshal(st.Inbox)
		if err != nil {
			return err
		}
		coreArgs = append(coreArgs, st.ID, int64(0), core)
		inventoryArgs = append(inventoryArgs, st.ID, inventory)
		inboxArgs = append(inboxArgs, st.ID, inbox)
	}
	tx, err := h.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	coreQuery := "INSERT INTO user_state_core(user_id,revision,payload) VALUES " + strings.TrimSuffix(strings.Repeat("(?,?,?),", len(states)), ",")
	if _, err := tx.ExecContext(ctx, coreQuery, coreArgs...); err != nil {
		return err
	}
	inventoryQuery := "INSERT INTO user_state_inventory(user_id,payload) VALUES " + strings.TrimSuffix(strings.Repeat("(?,?),", len(states)), ",")
	if _, err := tx.ExecContext(ctx, inventoryQuery, inventoryArgs...); err != nil {
		return err
	}
	inboxQuery := "INSERT INTO user_state_inbox(user_id,payload) VALUES " + strings.TrimSuffix(strings.Repeat("(?,?),", len(states)), ",")
	if _, err := tx.ExecContext(ctx, inboxQuery, inboxArgs...); err != nil {
		return err
	}
	return tx.Commit()
}
