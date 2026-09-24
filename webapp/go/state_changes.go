package main

type stateChanges struct {
	delta stateDelta

	decksOwned    bool
	bonusesOwned  bool
	historyOwned  bool
	cardsOwned    bool
	itemsOwned    bool
	dynamicOwned  bool
	receivedOwned bool
}

func newWorkingState(committed *userState) *userState {
	working := *committed
	working.base = committed
	working.changes = stateChanges{}
	return &working
}

func (st *userState) promoteCommitted(payloadBytes int) *userState {
	st.Revision++
	committed := *st
	committed.base = nil
	committed.changes = stateChanges{}
	if st.base != nil {
		committed.eventsSinceSnapshot = st.base.eventsSinceSnapshot + 1
		committed.eventBytes = st.base.eventBytes + payloadBytes
	} else {
		committed.eventsSinceSnapshot, committed.eventBytes = 1, payloadBytes
	}
	st.base = &committed
	st.changes = stateChanges{}
	return &committed
}

func addChangedRow[T any](rows *[]*T, changes *[]*T, owned *bool, row *T) {
	if !*owned {
		cloned := make([]*T, len(*rows), len(*rows)+1)
		copy(cloned, *rows)
		*rows = cloned
		*owned = true
	}
	*rows = append(*rows, row)
	*changes = append(*changes, row)
}

func editChangedRow[T any](rows *[]*T, changes *[]*T, owned *bool, id int64, rowID func(*T) int64) *T {
	for _, row := range *changes {
		if rowID(row) == id {
			return row
		}
	}
	for i, row := range *rows {
		if rowID(row) != id {
			continue
		}
		if !*owned {
			*rows = append([]*T(nil), (*rows)...)
			*owned = true
		}
		copy := *row
		(*rows)[i] = &copy
		*changes = append(*changes, &copy)
		return &copy
	}
	return nil
}

func (st *userState) editUser() *User {
	if st.base == nil || st.changes.delta.User != nil {
		return st.Core.User
	}
	copy := *st.Core.User
	st.Core.User = &copy
	st.changes.delta.User = &copy
	return &copy
}

func (st *userState) setBanned(value bool) {
	st.Core.Banned = value
	st.changes.delta.Banned = &value
}

func (st *userState) setToken(value *UserOneTimeToken) {
	st.Core.Token = value
	st.changes.delta.TokenChanged = true
	st.changes.delta.Token = value
}

func (st *userState) setSession(value *Session) {
	st.Core.Session = value
	st.changes.delta.SessionChanged = true
	st.changes.delta.Session = value
}

func (st *userState) addDeck(row *UserDeck) {
	addChangedRow(&st.Core.Decks, &st.changes.delta.Decks, &st.changes.decksOwned, row)
}

func (st *userState) editDeck(id int64) *UserDeck {
	return editChangedRow(&st.Core.Decks, &st.changes.delta.Decks, &st.changes.decksOwned, id, func(row *UserDeck) int64 { return row.ID })
}

func (st *userState) addBonus(row *UserLoginBonus) {
	addChangedRow(&st.Core.LoginBonuses, &st.changes.delta.Bonuses, &st.changes.bonusesOwned, row)
}

func (st *userState) editBonus(id int64) *UserLoginBonus {
	return editChangedRow(&st.Core.LoginBonuses, &st.changes.delta.Bonuses, &st.changes.bonusesOwned, id, func(row *UserLoginBonus) int64 { return row.ID })
}

func (st *userState) addPresentHistory(row *UserPresentAllReceivedHistory) {
	addChangedRow(&st.Core.PresentHistory, &st.changes.delta.History, &st.changes.historyOwned, row)
}

func (st *userState) addCard(row *UserCard) {
	addChangedRow(&st.Inventory.Cards, &st.changes.delta.Cards, &st.changes.cardsOwned, row)
}

func (st *userState) editCard(id int64) *UserCard {
	return editChangedRow(&st.Inventory.Cards, &st.changes.delta.Cards, &st.changes.cardsOwned, id, func(row *UserCard) int64 { return row.ID })
}

func (st *userState) addItem(row *UserItem) {
	addChangedRow(&st.Inventory.Items, &st.changes.delta.Items, &st.changes.itemsOwned, row)
}

func (st *userState) editItem(id int64) *UserItem {
	return editChangedRow(&st.Inventory.Items, &st.changes.delta.Items, &st.changes.itemsOwned, id, func(row *UserItem) int64 { return row.ID })
}

func (st *userState) addDynamic(row *UserPresent) {
	addChangedRow(&st.Inbox.Dynamic, &st.changes.delta.Dynamic, &st.changes.dynamicOwned, row)
}

func (st *userState) editDynamic(id int64) *UserPresent {
	return editChangedRow(&st.Inbox.Dynamic, &st.changes.delta.Dynamic, &st.changes.dynamicOwned, id, func(row *UserPresent) int64 { return row.ID })
}

func (st *userState) setReceived(id, at int64) {
	if !st.changes.receivedOwned {
		received := make(map[int64]int64, len(st.Inbox.Received)+1)
		for key, value := range st.Inbox.Received {
			received[key] = value
		}
		st.Inbox.Received = received
		st.changes.receivedOwned = true
	}
	st.Inbox.Received[id] = at
	if st.changes.delta.Received == nil {
		st.changes.delta.Received = make(map[int64]int64)
	}
	st.changes.delta.Received[id] = at
}
