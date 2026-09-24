package main

import (
	"reflect"
	"testing"
)

func TestBuildReceivedGrantPlan(t *testing.T) {
	amountPerSec := 12
	masters := map[int64]*ItemMaster{
		7:  {ID: 7, ItemType: 2, AmountPerSec: &amountPerSec},
		18: {ID: 18, ItemType: 3},
		27: {ID: 27, ItemType: 4},
	}
	presents := []*UserPresent{
		{UserID: 10, ItemType: 1, Amount: 20},
		{UserID: 10, ItemType: 3, ItemID: 18, Amount: 2},
		{UserID: 10, ItemType: 2, ItemID: 7, Amount: 1},
		{UserID: 10, ItemType: 3, ItemID: 18, Amount: 5},
		{UserID: 10, ItemType: 1, Amount: 30},
		{UserID: 10, ItemType: 2, ItemID: 7, Amount: 1},
		{UserID: 20, ItemType: 4, ItemID: 27, Amount: 3},
	}
	plan, err := buildReceivedGrantPlan(presents, masters)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.coins, map[int64]int64{10: 50}) {
		t.Fatalf("coins = %#v", plan.coins)
	}
	wantMaterials := map[receivedMaterialKey]receivedMaterialGrant{
		{userID: 10, itemID: 18}: {itemType: 3, amount: 7},
		{userID: 20, itemID: 27}: {itemType: 4, amount: 3},
	}
	if !reflect.DeepEqual(plan.materials, wantMaterials) {
		t.Fatalf("materials = %#v", plan.materials)
	}
	wantCards := []receivedCardGrant{
		{userID: 10, cardID: 7, amountPerSec: 12},
		{userID: 10, cardID: 7, amountPerSec: 12},
	}
	if !reflect.DeepEqual(plan.cards, wantCards) {
		t.Fatalf("cards = %#v", plan.cards)
	}
}

func TestBuildReceivedGrantPlanRejectsInvalidMaster(t *testing.T) {
	_, err := buildReceivedGrantPlan([]*UserPresent{{ItemType: 3, ItemID: 18}}, map[int64]*ItemMaster{18: {ID: 18, ItemType: 4}})
	if err != ErrItemNotFound {
		t.Fatalf("error = %v, want %v", err, ErrItemNotFound)
	}
}
