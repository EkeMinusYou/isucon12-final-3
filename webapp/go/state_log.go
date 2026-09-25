package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	stateBatchSize  = 128
	stateSyncPeriod = 100 * time.Millisecond
	maxStateFrame   = 16 << 20
)

type eventWriter struct {
	dir        string
	mu         sync.Mutex
	file       *os.File
	generation uint64
	failed     error
	unsynced   bool
	queue      chan *eventAppend
	done       chan struct{}
}

func stateDirectory() (string, error) {
	if dir := os.Getenv("ISUCON_STATE_DIR"); dir != "" {
		return dir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".isuconquest-state"), nil
}

func logPath(dir string, generation uint64) string {
	return filepath.Join(dir, fmt.Sprintf("events-%020d.log", generation))
}

func snapshotPath(dir string, generation uint64) string {
	return filepath.Join(dir, fmt.Sprintf("snapshot-%020d.dat", generation))
}

func seedsPath(dir string) string { return filepath.Join(dir, "seed-presents.dat") }

func generationFromName(name, prefix, suffix string) (uint64, bool) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	part := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if len(part) != 20 {
		return 0, false
	}
	n, err := strconv.ParseUint(part, 10, 64)
	return n, err == nil
}

func frame(payload []byte) ([]byte, error) {
	if len(payload) > maxStateFrame {
		return nil, fmt.Errorf("state record too large: %d", len(payload))
	}
	data := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(data[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(data[4:8], crc32.ChecksumIEEE(payload))
	copy(data[8:], payload)
	return data, nil
}

func recordPayload(id, revision int64, data []byte) []byte {
	payload := make([]byte, 16+len(data))
	binary.LittleEndian.PutUint64(payload[:8], uint64(id))
	binary.LittleEndian.PutUint64(payload[8:16], uint64(revision))
	copy(payload[16:], data)
	return payload
}

func readFrames(path string, allowTail bool, consume func([]byte) error) error {
	flags := os.O_RDONLY
	if allowTail {
		flags = os.O_RDWR
	}
	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	var offset int64
	for {
		var header [8]byte
		n, err := io.ReadFull(reader, header[:])
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF && allowTail {
			return truncateTail(f, offset)
		}
		if err != nil {
			return fmt.Errorf("read %s at %d (%d bytes): %w", path, offset, n, err)
		}
		length := binary.LittleEndian.Uint32(header[:4])
		if length < 16 || length > maxStateFrame {
			return fmt.Errorf("invalid state record size %d at %s:%d", length, path, offset)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if (err == io.EOF || err == io.ErrUnexpectedEOF) && allowTail {
				return truncateTail(f, offset)
			}
			return fmt.Errorf("read %s at %d: %w", path, offset, err)
		}
		if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(header[4:8]) {
			return fmt.Errorf("state checksum mismatch at %s:%d", path, offset)
		}
		if err := consume(payload); err != nil {
			return fmt.Errorf("apply %s at %d: %w", path, offset, err)
		}
		offset += int64(8 + length)
	}
}

func truncateTail(f *os.File, offset int64) error {
	if err := f.Truncate(offset); err != nil {
		return err
	}
	return f.Sync()
}

func recoverLocalState(dir string) (map[int64]*userState, uint64, error) {
	states := make(map[int64]*userState)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return states, 0, nil
		}
		return nil, 0, err
	}
	var snapshotGeneration uint64
	hasSnapshot := false
	logs := make([]uint64, 0)
	for _, entry := range entries {
		if n, ok := generationFromName(entry.Name(), "snapshot-", ".dat"); ok && (!hasSnapshot || n > snapshotGeneration) {
			snapshotGeneration, hasSnapshot = n, true
		}
		if n, ok := generationFromName(entry.Name(), "events-", ".log"); ok {
			logs = append(logs, n)
		}
	}
	if hasSnapshot {
		var expected int64 = -1
		err := readFrames(snapshotPath(dir, snapshotGeneration), false, func(payload []byte) error {
			id := int64(binary.LittleEndian.Uint64(payload[:8]))
			revision := int64(binary.LittleEndian.Uint64(payload[8:16]))
			if expected < 0 {
				if id != 0 || len(payload) != 16 || revision < 0 {
					return fmt.Errorf("invalid snapshot header")
				}
				expected = revision
				return nil
			}
			if id <= 0 || states[id] != nil {
				return fmt.Errorf("duplicate or invalid snapshot user %d", id)
			}
			var state statePayload
			if err := json.Unmarshal(payload[16:], &state); err != nil {
				return err
			}
			if state.Core.User == nil || state.Core.User.ID != id {
				return fmt.Errorf("invalid snapshot user %d", id)
			}
			states[id] = &userState{ID: id, Revision: revision, Core: state.Core, Inventory: state.Inventory, Inbox: state.Inbox}
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
		if expected < 0 || int64(len(states)) != expected {
			return nil, 0, fmt.Errorf("incomplete snapshot: got %d users, expected %d", len(states), expected)
		}
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i] < logs[j] })
	current := snapshotGeneration
	lastLog := snapshotGeneration
	hasLog := false
	for _, generation := range logs {
		if hasSnapshot && generation < snapshotGeneration {
			continue
		}
		if (hasLog && generation != lastLog+1) || (!hasLog && generation != snapshotGeneration) {
			return nil, 0, fmt.Errorf("missing state log generation before %d", generation)
		}
		hasLog, lastLog = true, generation
		if generation > current {
			current = generation
		}
		if err := readFrames(logPath(dir, generation), generation == logs[len(logs)-1], func(payload []byte) error {
			id := int64(binary.LittleEndian.Uint64(payload[:8]))
			revision := int64(binary.LittleEndian.Uint64(payload[8:16]))
			st := states[id]
			if st != nil && revision != st.Revision+1 {
				return fmt.Errorf("state revision gap: user=%d expected=%d got=%d", id, st.Revision+1, revision)
			}
			if st == nil && revision != 1 {
				return fmt.Errorf("missing create event: user=%d seq=%d", id, revision)
			}
			var delta stateDelta
			if err := json.Unmarshal(payload[16:], &delta); err != nil {
				return err
			}
			if st == nil {
				if delta.Create == nil {
					return fmt.Errorf("missing create event: user=%d", id)
				}
				st = &userState{ID: id}
				states[id] = st
			} else if delta.Create != nil {
				return fmt.Errorf("duplicate create event: user=%d", id)
			}
			if err := applyStateDelta(st, delta); err != nil {
				return err
			}
			if st.Core.User == nil || st.Core.User.ID != id {
				return fmt.Errorf("invalid event user %d", id)
			}
			st.Revision = revision
			return nil
		}); err != nil {
			return nil, 0, err
		}
	}
	return states, current, nil
}

func readLocalSeeds(dir string) (map[int64][]*UserPresent, error) {
	seeds := make(map[int64][]*UserPresent)
	path := seedsPath(dir)
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		entries, readErr := os.ReadDir(dir)
		if os.IsNotExist(readErr) {
			return seeds, nil
		}
		if readErr != nil {
			return nil, readErr
		}
		for _, entry := range entries {
			if _, ok := generationFromName(entry.Name(), "snapshot-", ".dat"); ok {
				return nil, fmt.Errorf("missing seed presents file beside state snapshot")
			}
		}
		return seeds, nil
	}
	var expected int64 = -1
	err := readFrames(path, false, func(payload []byte) error {
		id := int64(binary.LittleEndian.Uint64(payload[:8]))
		count := int64(binary.LittleEndian.Uint64(payload[8:16]))
		if expected < 0 {
			if id != 0 || len(payload) != 16 || count < 0 {
				return fmt.Errorf("invalid seed presents header")
			}
			expected = count
			return nil
		}
		if _, exists := seeds[id]; id <= 0 || exists || count < 0 {
			return fmt.Errorf("duplicate or invalid seed owner %d", id)
		}
		var presents []*UserPresent
		if err := json.Unmarshal(payload[16:], &presents); err != nil {
			return err
		}
		if int64(len(presents)) != count {
			return fmt.Errorf("seed present count mismatch: user=%d", id)
		}
		for _, present := range presents {
			if present == nil || present.UserID != id {
				return fmt.Errorf("invalid seed present owner %d", id)
			}
		}
		seeds[id] = presents
		return nil
	})
	if err != nil {
		return nil, err
	}
	if expected < 0 || int64(len(seeds)) != expected {
		return nil, fmt.Errorf("incomplete seed presents file")
	}
	return seeds, nil
}

func newEventWriter(dir string) (*eventWriter, map[int64]*userState, map[int64][]*UserPresent, error) {
	states, generation, err := recoverLocalState(dir)
	if err != nil {
		return nil, nil, nil, err
	}
	seeds, err := readLocalSeeds(dir)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, nil, nil, err
	}
	f, err := os.OpenFile(logPath(dir, generation), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := syncDirectory(dir); err != nil {
		f.Close()
		return nil, nil, nil, err
	}
	w := &eventWriter{dir: dir, file: f, generation: generation, queue: make(chan *eventAppend, 4096), done: make(chan struct{})}
	go w.run()
	return w, states, seeds, nil
}

func (w *eventWriter) append(req *eventAppend) error {
	w.queue <- req
	return <-req.done
}

func (w *eventWriter) run() {
	defer close(w.done)
	ticker := time.NewTicker(stateSyncPeriod)
	defer ticker.Stop()
	lastReport := time.Now()
	var batches, events, writtenBytes uint64
	var syncs uint64
	var syncTime time.Duration
	var maxBatch int
	var syncFailureReported bool
	for {
		var first *eventAppend
		select {
		case req, ok := <-w.queue:
			if !ok {
				if _, err := w.syncPending(); err != nil {
					log.Printf("state log final sync: %v", err)
				}
				return
			}
			first = req
		case <-ticker.C:
			duration, err := w.syncPending()
			if duration > 0 {
				syncs++
				syncTime += duration
			}
			if err != nil && !syncFailureReported {
				log.Printf("state log periodic sync: %v", err)
				syncFailureReported = true
			} else if err == nil {
				syncFailureReported = false
			}
			continue
		}
		batch := []*eventAppend{first}
	collect:
		for len(batch) < stateBatchSize {
			select {
			case req, ok := <-w.queue:
				if !ok {
					break collect
				}
				batch = append(batch, req)
			default:
				break collect
			}
		}
		bytes, err := w.writeBatch(batch)
		for _, req := range batch {
			req.done <- err
		}
		if err == nil {
			batches++
			events += uint64(len(batch))
			writtenBytes += uint64(bytes)
			if len(batch) > maxBatch {
				maxBatch = len(batch)
			}
		}
		if time.Since(lastReport) >= 10*time.Second {
			log.Printf("state log: batches=%d events=%d bytes=%d max_batch=%d syncs=%d sync_ms=%d", batches, events, writtenBytes, maxBatch, syncs, syncTime.Milliseconds())
			lastReport = time.Now()
			batches, events, writtenBytes, syncs, syncTime, maxBatch = 0, 0, 0, 0, 0, 0
		}
	}
}

func (w *eventWriter) close() error {
	close(w.queue)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		if err := w.file.Close(); err != nil && w.failed == nil {
			return err
		}
	}
	return w.failed
}

func (w *eventWriter) writeBatch(batch []*eventAppend) (int, error) {
	var data bytes.Buffer
	for _, req := range batch {
		entry, err := frame(recordPayload(req.userID, req.seq, req.payload))
		if err != nil {
			return 0, err
		}
		data.Write(entry)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, w.failed
	}
	if w.file == nil {
		return 0, fmt.Errorf("state log is closed")
	}
	n, err := w.file.Write(data.Bytes())
	if err == nil && n != data.Len() {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.failed = fmt.Errorf("append state log: %w", err)
	} else {
		w.unsynced = true
	}
	return n, w.failed
}

func (w *eventWriter) syncPending() (time.Duration, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *eventWriter) syncLocked() (time.Duration, error) {
	if w.failed != nil {
		return 0, w.failed
	}
	if w.file == nil {
		w.failed = fmt.Errorf("state log is closed")
		return 0, w.failed
	}
	if !w.unsynced {
		return 0, nil
	}
	start := time.Now()
	err := w.file.Sync()
	duration := time.Since(start)
	if err != nil {
		w.failed = fmt.Errorf("sync state log: %w", err)
		return duration, w.failed
	}
	w.unsynced = false
	return duration, nil
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func writeSnapshot(dir string, generation uint64, states map[int64]*userState) (err error) {
	path := snapshotPath(dir, generation)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	writer := bufio.NewWriterSize(f, 1<<20)
	ids := make([]int64, 0, len(states))
	for id := range states {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	header, err := frame(recordPayload(0, int64(len(ids)), nil))
	if err != nil {
		return err
	}
	if _, err := writer.Write(header); err != nil {
		return err
	}
	for _, id := range ids {
		st := states[id]
		payload, marshalErr := json.Marshal(statePayload{Core: st.Core, Inventory: st.Inventory, Inbox: st.Inbox})
		if marshalErr != nil {
			return marshalErr
		}
		entry, frameErr := frame(recordPayload(id, st.Revision, payload))
		if frameErr != nil {
			return frameErr
		}
		if _, err := writer.Write(entry); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func writeLocalSeeds(dir string, seeds map[int64][]*UserPresent) (err error) {
	path := seedsPath(dir)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	writer := bufio.NewWriterSize(f, 1<<20)
	ids := make([]int64, 0, len(seeds))
	for id := range seeds {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	header, err := frame(recordPayload(0, int64(len(ids)), nil))
	if err != nil {
		return err
	}
	if _, err := writer.Write(header); err != nil {
		return err
	}
	for _, id := range ids {
		data, marshalErr := json.Marshal(seeds[id])
		if marshalErr != nil {
			return marshalErr
		}
		entry, frameErr := frame(recordPayload(id, int64(len(seeds[id])), data))
		if frameErr != nil {
			return frameErr
		}
		if _, err := writer.Write(entry); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func (w *eventWriter) reset(states map[int64]*userState, seeds map[int64][]*UserPresent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
		w.unsynced = false
	}
	if err := os.RemoveAll(w.dir); err != nil {
		return err
	}
	if err := os.MkdirAll(w.dir, 0700); err != nil {
		return err
	}
	if err := writeLocalSeeds(w.dir, seeds); err != nil {
		return err
	}
	if err := writeSnapshot(w.dir, 0, states); err != nil {
		return err
	}
	f, err := os.OpenFile(logPath(w.dir, 0), os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if err := syncDirectory(w.dir); err != nil {
		f.Close()
		return err
	}
	w.file, w.generation, w.failed, w.unsynced = f, 0, nil, false
	return nil
}

func (w *eventWriter) rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, w.failed
	}
	if _, err := w.syncLocked(); err != nil {
		return 0, err
	}
	next := w.generation + 1
	f, err := os.OpenFile(logPath(w.dir, next), os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return 0, err
	}
	if err := syncDirectory(w.dir); err != nil {
		f.Close()
		return 0, err
	}
	if err := w.file.Close(); err != nil {
		f.Close()
		return 0, err
	}
	w.file, w.generation, w.unsynced = f, next, false
	return next, nil
}

func (w *eventWriter) discardBefore(generation uint64) error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if n, ok := generationFromName(name, "snapshot-", ".dat"); ok && n < generation {
			if err := os.Remove(filepath.Join(w.dir, name)); err != nil {
				return err
			}
		}
		if n, ok := generationFromName(name, "events-", ".log"); ok && n < generation {
			if err := os.Remove(filepath.Join(w.dir, name)); err != nil {
				return err
			}
		}
	}
	return syncDirectory(w.dir)
}

func (h *Handler) checkpointLocal() error {
	h.CheckpointMu.Lock()
	defer h.CheckpointMu.Unlock()
	if !h.LastReset.IsZero() && time.Since(h.LastReset) < 2*time.Minute {
		return nil
	}
	h.State.gate.Lock()
	states := h.State.snapshot()
	generation, err := h.Writer.rotate()
	h.State.gate.Unlock()
	if err != nil {
		return err
	}
	if err := writeSnapshot(h.Writer.dir, generation, states); err != nil {
		return err
	}
	return h.Writer.discardBefore(generation)
}
