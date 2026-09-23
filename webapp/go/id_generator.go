package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const (
	// Initialization removes dynamically created presents above this boundary.
	firstGeneratedID  int64 = 100_000_000_001
	idReservationSize int64 = 1_000_000
	maxSafeJSONID     int64 = 1<<53 - 1
)

type idAllocator struct {
	mu       sync.Mutex
	state    string
	next     int64
	reserved int64
}

func newIDAllocator() (*idAllocator, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	return newIDAllocatorAt(filepath.Join(home, ".isuconquest-id-state"))
}

func newIDAllocatorAt(state string) (*idAllocator, error) {
	a := &idAllocator{state: state}
	if err := a.reserve(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *idAllocator) nextID() (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.next == a.reserved {
		if err := a.reserve(); err != nil {
			return 0, err
		}
	}
	id := a.next
	a.next++
	return id, nil
}

func (a *idAllocator) reserve() error {
	lock, err := os.OpenFile(a.state+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("open ID lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock ID state: %w", err)
	}

	start := firstGeneratedID
	data, err := os.ReadFile(a.state)
	if err == nil {
		start, err = strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid ID state: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read ID state: %w", err)
	}
	if start < firstGeneratedID || start > maxSafeJSONID-idReservationSize {
		return fmt.Errorf("ID state is out of range: %d", start)
	}
	end := start + idReservationSize

	dir := filepath.Dir(a.state)
	tmp, err := os.CreateTemp(dir, ".isuconquest-id-state-*")
	if err != nil {
		return fmt.Errorf("create ID state: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := fmt.Fprintln(tmp, end); err != nil {
		tmp.Close()
		return fmt.Errorf("write ID state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync ID state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close ID state: %w", err)
	}
	if err := os.Rename(tmp.Name(), a.state); err != nil {
		return fmt.Errorf("replace ID state: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open ID state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync ID state directory: %w", err)
	}
	a.next = start
	a.reserved = end
	return nil
}
