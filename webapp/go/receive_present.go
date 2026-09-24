package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"
)

const receivedItemsBatchSize = 1000

type receivedMaterialKey struct {
	userID int64
	itemID int64
}

type receivedCardGrant struct {
	userID       int64
	cardID       int64
	amountPerSec int
}

type receivedMaterialGrant struct {
	itemType int
	amount   int64
}

type receivedGrantPlan struct {
	coins     map[int64]int64
	materials map[receivedMaterialKey]receivedMaterialGrant
	cards     []receivedCardGrant
}

func buildReceivedGrantPlan(presents []*UserPresent, masters map[int64]*ItemMaster) (receivedGrantPlan, error) {
	plan := receivedGrantPlan{
		coins:     make(map[int64]int64),
		materials: make(map[receivedMaterialKey]receivedMaterialGrant),
	}
	for _, present := range presents {
		switch present.ItemType {
		case 1:
			plan.coins[present.UserID] += int64(present.Amount)
		case 2, 3, 4:
			item, ok := masters[present.ItemID]
			if !ok || item.ItemType != present.ItemType {
				return receivedGrantPlan{}, ErrItemNotFound
			}
			if present.ItemType == 2 {
				if item.AmountPerSec == nil {
					return receivedGrantPlan{}, fmt.Errorf("card master %d has no amount_per_sec", item.ID)
				}
				plan.cards = append(plan.cards, receivedCardGrant{
					userID:       present.UserID,
					cardID:       item.ID,
					amountPerSec: *item.AmountPerSec,
				})
			} else {
				key := receivedMaterialKey{userID: present.UserID, itemID: item.ID}
				grant := plan.materials[key]
				grant.itemType = item.ItemType
				grant.amount += int64(present.Amount)
				plan.materials[key] = grant
			}
		default:
			return receivedGrantPlan{}, ErrInvalidItemType
		}
	}
	return plan, nil
}

func (h *Handler) obtainReceivedItems(tx *sqlx.Tx, presents []*UserPresent, requestAt int64) error {
	masterIDs := make([]int64, 0)
	seenMasters := make(map[int64]struct{})
	for _, present := range presents {
		if present.ItemType < 2 || present.ItemType > 4 {
			continue
		}
		if _, ok := seenMasters[present.ItemID]; !ok {
			seenMasters[present.ItemID] = struct{}{}
			masterIDs = append(masterIDs, present.ItemID)
		}
	}
	sort.Slice(masterIDs, func(i, j int) bool { return masterIDs[i] < masterIDs[j] })

	masters := make(map[int64]*ItemMaster, len(masterIDs))
	for start := 0; start < len(masterIDs); start += receivedItemsBatchSize {
		end := start + receivedItemsBatchSize
		if end > len(masterIDs) {
			end = len(masterIDs)
		}
		query, args, err := sqlx.In("SELECT id, item_type, amount_per_sec FROM item_masters WHERE id IN (?)", masterIDs[start:end])
		if err != nil {
			return err
		}
		var rows []*ItemMaster
		if err := tx.Select(&rows, query, args...); err != nil {
			return err
		}
		for _, item := range rows {
			masters[item.ID] = item
		}
	}

	plan, err := buildReceivedGrantPlan(presents, masters)
	if err != nil {
		return err
	}
	if err := grantReceivedCoins(tx, plan.coins); err != nil {
		return err
	}
	if err := h.grantReceivedMaterials(tx, plan.materials, requestAt); err != nil {
		return err
	}
	return h.grantReceivedCards(tx, plan.cards, requestAt)
}

func grantReceivedCoins(tx *sqlx.Tx, coins map[int64]int64) error {
	if len(coins) == 0 {
		return nil
	}
	users := make([]int64, 0, len(coins))
	for userID := range coins {
		users = append(users, userID)
	}
	sort.Slice(users, func(i, j int) bool { return users[i] < users[j] })
	for start := 0; start < len(users); start += receivedItemsBatchSize {
		end := start + receivedItemsBatchSize
		if end > len(users) {
			end = len(users)
		}
		query, args, err := sqlx.In("SELECT id FROM users WHERE id IN (?)", users[start:end])
		if err != nil {
			return err
		}
		var found []int64
		if err := tx.Select(&found, query, args...); err != nil {
			return err
		}
		if len(found) != end-start {
			return ErrUserNotFound
		}
	}
	for _, userID := range users {
		if _, err := tx.Exec("UPDATE users SET isu_coin=isu_coin+? WHERE id=?", coins[userID], userID); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) grantReceivedMaterials(tx *sqlx.Tx, materials map[receivedMaterialKey]receivedMaterialGrant, requestAt int64) error {
	if len(materials) == 0 {
		return nil
	}
	itemsByUser := make(map[int64][]int64)
	keys := make([]receivedMaterialKey, 0, len(materials))
	for key := range materials {
		itemsByUser[key.userID] = append(itemsByUser[key.userID], key.itemID)
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].userID != keys[j].userID {
			return keys[i].userID < keys[j].userID
		}
		return keys[i].itemID < keys[j].itemID
	})

	selected := make(map[receivedMaterialKey]int64, len(materials))
	for userID, itemIDs := range itemsByUser {
		sort.Slice(itemIDs, func(i, j int) bool { return itemIDs[i] < itemIDs[j] })
		for start := 0; start < len(itemIDs); start += receivedItemsBatchSize {
			end := start + receivedItemsBatchSize
			if end > len(itemIDs) {
				end = len(itemIDs)
			}
			query, args, err := sqlx.In("SELECT id, item_id FROM user_items WHERE user_id=? AND item_id IN (?) ORDER BY item_id, id", userID, itemIDs[start:end])
			if err != nil {
				return err
			}
			var rows []struct {
				ID     int64 `db:"id"`
				ItemID int64 `db:"item_id"`
			}
			if err := tx.Select(&rows, query, args...); err != nil {
				return err
			}
			for _, row := range rows {
				key := receivedMaterialKey{userID: userID, itemID: row.ItemID}
				if _, ok := selected[key]; !ok {
					selected[key] = row.ID
				}
			}
		}
	}

	newItems := make([]*UserItem, 0)
	for _, key := range keys {
		grant := materials[key]
		if id, ok := selected[key]; ok {
			if _, err := tx.Exec("UPDATE user_items SET amount=amount+?, updated_at=? WHERE id=?", grant.amount, requestAt, id); err != nil {
				return err
			}
			continue
		}
		id, err := h.generateID()
		if err != nil {
			return err
		}
		newItems = append(newItems, &UserItem{
			ID:        id,
			UserID:    key.userID,
			ItemType:  grant.itemType,
			ItemID:    key.itemID,
			Amount:    int(grant.amount),
			CreatedAt: requestAt,
			UpdatedAt: requestAt,
		})
	}
	return insertReceivedMaterials(tx, newItems)
}

func insertReceivedMaterials(tx *sqlx.Tx, items []*UserItem) error {
	const prefix = "INSERT INTO user_items(id, user_id, item_id, item_type, amount, created_at, updated_at) VALUES "
	const row = "(?, ?, ?, ?, ?, ?, ?)"
	for start := 0; start < len(items); start += receivedItemsBatchSize {
		end := start + receivedItemsBatchSize
		if end > len(items) {
			end = len(items)
		}
		var query strings.Builder
		query.Grow(len(prefix) + (end-start)*(len(row)+1))
		query.WriteString(prefix)
		args := make([]interface{}, 0, (end-start)*7)
		for i, item := range items[start:end] {
			if i > 0 {
				query.WriteByte(',')
			}
			query.WriteString(row)
			args = append(args, item.ID, item.UserID, item.ItemID, item.ItemType, item.Amount, item.CreatedAt, item.UpdatedAt)
		}
		if _, err := tx.Exec(query.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) grantReceivedCards(tx *sqlx.Tx, cards []receivedCardGrant, requestAt int64) error {
	const prefix = "INSERT INTO user_cards(id, user_id, card_id, amount_per_sec, level, total_exp, created_at, updated_at) VALUES "
	const row = "(?, ?, ?, ?, ?, ?, ?, ?)"
	for start := 0; start < len(cards); start += receivedItemsBatchSize {
		end := start + receivedItemsBatchSize
		if end > len(cards) {
			end = len(cards)
		}
		var query strings.Builder
		query.Grow(len(prefix) + (end-start)*(len(row)+1))
		query.WriteString(prefix)
		args := make([]interface{}, 0, (end-start)*8)
		for i, card := range cards[start:end] {
			id, err := h.generateID()
			if err != nil {
				return err
			}
			if i > 0 {
				query.WriteByte(',')
			}
			query.WriteString(row)
			args = append(args, id, card.userID, card.cardID, card.amountPerSec, 1, 0, requestAt, requestAt)
		}
		if _, err := tx.Exec(query.String(), args...); err != nil {
			return err
		}
	}
	return nil
}
