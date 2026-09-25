package main

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func loadInitialRows[T any](ctx context.Context, db *sqlx.DB, table, key string, shard int, add func(*T) error) error {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE ((%s-1) %% 5 = ? OR ((%s-1) %% 5 = 0 AND ((%s-1) DIV 5) %% 4 = ?)) ORDER BY id`, table, key, key, key)
	rows, err := db.QueryxContext(ctx, query, shard, shard-1)
	if err != nil {
		return fmt.Errorf("load %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		row := new(T)
		if err := rows.StructScan(row); err != nil {
			return fmt.Errorf("scan %s: %w", table, err)
		}
		if err := add(row); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read %s: %w", table, err)
	}
	return nil
}

func (h *Handler) loadInitialStates(ctx context.Context) (map[int64]*userState, error) {
	states := make(map[int64]*userState)
	if h.Cluster.Self == 0 {
		return states, nil
	}
	shard := h.Cluster.Self
	if err := loadInitialRows[User](ctx, h.DB, "users", "id", shard, func(row *User) error {
		states[row.ID] = &userState{ID: row.ID, Core: stateCore{User: row}}
		return nil
	}); err != nil {
		return nil, err
	}
	state := func(table string, id int64) (*userState, error) {
		st := states[id]
		if st == nil {
			return nil, fmt.Errorf("%s refers to missing user %d", table, id)
		}
		return st, nil
	}
	if err := loadInitialRows[UserDevice](ctx, h.DB, "user_devices", "user_id", shard, func(row *UserDevice) error {
		st, err := state("user_devices", row.UserID)
		if err != nil {
			return err
		}
		st.Core.Devices = append(st.Core.Devices, row)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := loadInitialRows[UserDeck](ctx, h.DB, "user_decks", "user_id", shard, func(row *UserDeck) error {
		st, err := state("user_decks", row.UserID)
		if err != nil {
			return err
		}
		st.Core.Decks = append(st.Core.Decks, row)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := loadInitialRows[UserLoginBonus](ctx, h.DB, "user_login_bonuses", "user_id", shard, func(row *UserLoginBonus) error {
		st, err := state("user_login_bonuses", row.UserID)
		if err != nil {
			return err
		}
		st.Core.LoginBonuses = append(st.Core.LoginBonuses, row)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := loadInitialRows[UserPresentAllReceivedHistory](ctx, h.DB, "user_present_all_received_history", "user_id", shard, func(row *UserPresentAllReceivedHistory) error {
		st, err := state("user_present_all_received_history", row.UserID)
		if err != nil {
			return err
		}
		st.Core.PresentHistory = append(st.Core.PresentHistory, row)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := loadInitialRows[UserCard](ctx, h.DB, "user_cards", "user_id", shard, func(row *UserCard) error {
		st, err := state("user_cards", row.UserID)
		if err != nil {
			return err
		}
		st.Inventory.Cards = append(st.Inventory.Cards, row)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := loadInitialRows[UserItem](ctx, h.DB, "user_items", "user_id", shard, func(row *UserItem) error {
		st, err := state("user_items", row.UserID)
		if err != nil {
			return err
		}
		st.Inventory.Items = append(st.Inventory.Items, row)
		return nil
	}); err != nil {
		return nil, err
	}
	query := `SELECT user_id FROM user_bans WHERE ((user_id-1) % 5 = ? OR ((user_id-1) % 5 = 0 AND ((user_id-1) DIV 5) % 4 = ?))`
	rows, err := h.DB.QueryxContext(ctx, query, shard, shard-1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		st, err := state("user_bans", id)
		if err != nil {
			return nil, err
		}
		st.Core.Banned = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return states, nil
}
