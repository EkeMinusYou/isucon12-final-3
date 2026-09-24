package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestIDAllocatorPersistsReservedRanges(t *testing.T) {
	state := filepath.Join(t.TempDir(), "ids")
	first, err := newIDAllocatorAt(state, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := first.nextID()
	if err != nil {
		t.Fatal(err)
	}
	if firstID != firstGeneratedID {
		t.Fatalf("first ID = %d, want %d", firstID, firstGeneratedID)
	}

	// A second process must skip every ID reserved by the first process.
	second, err := newIDAllocatorAt(state, 0)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := second.nextID()
	if err != nil {
		t.Fatal(err)
	}
	if secondID != firstGeneratedID+idReservationSize*idShardCount {
		t.Fatalf("second process ID = %d, want %d", secondID, firstGeneratedID+idReservationSize*idShardCount)
	}
}

func TestIDAllocatorConcurrentProcesses(t *testing.T) {
	state := filepath.Join(t.TempDir(), "ids")
	const processes = 16
	ids := make(chan int64, processes)
	errs := make(chan error, processes)
	var wg sync.WaitGroup
	for i := 0; i < processes; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allocator, err := newIDAllocatorAt(state, 0)
			if err == nil {
				var id int64
				id, err = allocator.nextID()
				if err == nil {
					ids <- id
				}
			}
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[int64]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate ID: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != processes {
		t.Fatalf("got %d IDs, want %d", len(seen), processes)
	}
}

func TestIDAllocatorRejectsCorruptState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "ids")
	if err := os.WriteFile(state, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := newIDAllocatorAt(state, 0); err == nil {
		t.Fatal("expected corrupt state to prevent ID reuse")
	}
}

func TestIDAllocatorShards(t *testing.T) {
	seen := make(map[int64]bool)
	for shard := 0; shard < int(idShardCount); shard++ {
		a, err := newIDAllocatorAt(filepath.Join(t.TempDir(), "ids"), shard)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			id, err := a.nextID()
			if err != nil {
				t.Fatal(err)
			}
			if int((id-1)%idShardCount) != shard || seen[id] {
				t.Fatalf("invalid or duplicate ID %d on shard %d", id, shard)
			}
			seen[id] = true
		}
	}
}
