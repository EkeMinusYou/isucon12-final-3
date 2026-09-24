package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
)

type masterSnapshot struct {
	Version      *VersionMaster
	Items        map[int64]*ItemMaster
	Gachas       []*GachaMaster
	GachaItems   map[int64][]*GachaItemMaster
	Bonuses      []*LoginBonusMaster
	BonusRewards map[int64]map[int]*LoginBonusRewardMaster
	PresentAll   []*PresentAllMaster
}

type masterStore struct {
	mu       sync.RWMutex
	updateMu sync.Mutex
	value    *masterSnapshot
	revision int64
}

func (m *masterStore) clear() {
	m.mu.Lock()
	m.value = nil
	m.revision = 0
	m.mu.Unlock()
}

func (m *masterStore) get(db *sqlx.DB) (*masterSnapshot, error) {
	m.mu.RLock()
	if m.value != nil {
		value := m.value
		m.mu.RUnlock()
		return value, nil
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.value != nil {
		return m.value, nil
	}
	return m.reloadLocked(db)
}

func (m *masterStore) refresh(db *sqlx.DB, expected int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.value != nil && m.revision >= expected {
		return nil
	}
	m.value = nil
	_, err := m.reloadLocked(db)
	if err != nil {
		return err
	}
	if m.revision < expected {
		m.value = nil
		return fmt.Errorf("master revision %d is older than %d", m.revision, expected)
	}
	return nil
}

func (m *masterStore) reloadLocked(db *sqlx.DB) (*masterSnapshot, error) {
	tx, err := db.BeginTxx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	var revision int64
	if err := tx.Get(&revision, "SELECT revision FROM master_revision WHERE id=1"); err != nil {
		return nil, err
	}
	s := &masterSnapshot{}
	s.Version = new(VersionMaster)
	if err := tx.Get(s.Version, "SELECT * FROM version_masters WHERE status=1"); err != nil {
		return nil, err
	}
	items := make([]*ItemMaster, 0)
	if err := tx.Select(&items, "SELECT * FROM item_masters"); err != nil {
		return nil, err
	}
	s.Items = make(map[int64]*ItemMaster, len(items))
	for _, v := range items {
		s.Items[v.ID] = v
	}
	if err := tx.Select(&s.Gachas, "SELECT * FROM gacha_masters ORDER BY display_order ASC, id ASC"); err != nil {
		return nil, err
	}
	allGachaItems := make([]*GachaItemMaster, 0)
	if err := tx.Select(&allGachaItems, "SELECT * FROM gacha_item_masters ORDER BY id ASC"); err != nil {
		return nil, err
	}
	s.GachaItems = make(map[int64][]*GachaItemMaster)
	for _, v := range allGachaItems {
		s.GachaItems[v.GachaID] = append(s.GachaItems[v.GachaID], v)
	}
	if err := tx.Select(&s.Bonuses, "SELECT * FROM login_bonus_masters ORDER BY id ASC"); err != nil {
		return nil, err
	}
	rewards := make([]*LoginBonusRewardMaster, 0)
	if err := tx.Select(&rewards, "SELECT * FROM login_bonus_reward_masters"); err != nil {
		return nil, err
	}
	s.BonusRewards = make(map[int64]map[int]*LoginBonusRewardMaster)
	for _, v := range rewards {
		if s.BonusRewards[v.LoginBonusID] == nil {
			s.BonusRewards[v.LoginBonusID] = make(map[int]*LoginBonusRewardMaster)
		}
		s.BonusRewards[v.LoginBonusID][v.RewardSequence] = v
	}
	if err := tx.Select(&s.PresentAll, "SELECT * FROM present_all_masters ORDER BY id ASC"); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	m.value = s
	m.revision = revision
	return s, nil
}

func (s *masterSnapshot) activeGachas(at int64) []*GachaMaster {
	result := make([]*GachaMaster, 0)
	for _, v := range s.Gachas {
		if v.StartAt <= at && v.EndAt >= at {
			result = append(result, v)
		}
	}
	return result
}

func (s *masterSnapshot) gacha(id int64, at int64) *GachaMaster {
	for _, v := range s.Gachas {
		if v.ID == id && v.StartAt <= at && v.EndAt >= at {
			return v
		}
	}
	return nil
}

func (s *masterSnapshot) activeBonuses(at int64) []*LoginBonusMaster {
	result := make([]*LoginBonusMaster, 0)
	for _, v := range s.Bonuses {
		if v.StartAt <= at && v.EndAt >= at {
			result = append(result, v)
		}
	}
	return result
}

func (s *masterSnapshot) activePresents(at int64) []*PresentAllMaster {
	result := make([]*PresentAllMaster, 0)
	for _, v := range s.PresentAll {
		if v.RegisteredStartAt <= at && v.RegisteredEndAt >= at {
			result = append(result, v)
		}
	}
	return result
}
