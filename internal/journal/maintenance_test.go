package journal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/model"
)

func TestOfflineExportConcurrentPublication(t *testing.T) {
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")}
	for i, path := range paths {
		s, err := Open(path, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.SetMeta("instance_id", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	output := filepath.Join(dir, "backup.db")
	start := make(chan struct{})
	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := range paths {
		wg.Go(func() { <-start; results[i] = ExportOffline(paths[i], output, false) })
	}
	close(start)
	wg.Wait()
	if (results[0] == nil) == (results[1] == nil) {
		t.Fatal("expected exactly one published export", results)
	}
	winner := "0"
	if results[1] == nil {
		winner = "1"
	}
	s, err := Open(output, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var owner string
	if err = s.Meta("instance_id", &owner); err != nil || owner != winner {
		t.Fatal("published backup overwritten", owner, winner, err)
	}
}

func TestOfflineExportFullDisk(t *testing.T) {
	root := os.Getenv("MIGRES_DISK_FAULT_DIR")
	if root == "" {
		t.Skip("requires a dedicated 4 MiB tmpfs")
	}
	dir, err := os.MkdirTemp(root, "export-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(t.TempDir(), "journal.db")
	s, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append([]model.Event{{Kind: "insert", Query: strings.Repeat("x", 512<<10)}}, model.Position{File: "bin.000001", Pos: 4}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	filler := filepath.Join(dir, "filler")
	if err = os.WriteFile(filler, bytes.Repeat([]byte{'x'}, (4<<20)-(128<<10)), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "backup.db")
	if err = ExportOffline(path, output, false); err == nil {
		t.Fatal("export unexpectedly fit on full disk")
	}
	if _, err = os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("published incomplete backup", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, ".migres-journal-*"))
	if err != nil || len(files) != 0 {
		t.Fatal("failed export left temporary payload", files, err)
	}
	if err = os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	if err = ExportOffline(path, output, false); err != nil {
		t.Fatal("retry after freeing disk", err)
	}
	r, err := Open(output, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	events, err := r.Events(1)
	if err != nil || len(events) != 1 || len(events[0].Query) != 512<<10 {
		t.Fatal("retry lost original data", err)
	}
}

func TestOfflineBackupRestore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.db")
	s, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	e := model.Event{Kind: "insert", Table: "orders", SourceID: "bin.000001:10"}
	pos := model.Position{File: "bin.000001", Pos: 10}
	if err = s.Append([]model.Event{e}, pos); err != nil {
		t.Fatal(err)
	}
	events, _ := s.Events(10)
	if err = s.Transform(events[0].Sequence, []model.Task{{Table: "orders", Kind: "insert", Rows: []model.Row{{Version: events[0].Sequence}}}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Append([]model.Event{{Kind: "insert", Table: "later"}}, pos); err != nil {
		t.Fatal(err)
	}
	if err = s.Stage(model.Event{Kind: "insert", Table: "uncommitted"}); err != nil {
		t.Fatal(err)
	}
	if err = s.SetMeta("instance_id", "persistent-owner"); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveSchema(model.Schema{Table: "orders", Generation: "generation"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Operation(model.Operation{ID: "operation", State: "applying"}); err != nil {
		t.Fatal(err)
	}
	tasks, _ := s.Tasks(10)
	backup := filepath.Join(dir, "backup.db")
	if err = ExportOffline(path, backup, false); err == nil {
		t.Fatal("backed up a live journal")
	}
	s.Close()
	if err = ExportOffline(path, backup, false); err != nil {
		t.Fatal(err)
	}
	if err = ExportOffline(path, backup, false); err == nil {
		t.Fatal("overwrote backup")
	}
	for _, compact := range []bool{false, true} {
		restored := filepath.Join(dir, fmt.Sprintf("restored-%t.db", compact))
		if err = ExportOffline(backup, restored, compact); err != nil {
			t.Fatal(err)
		}
		r, err := Open(restored, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := r.Tasks(10)
		if !reflect.DeepEqual(tasks, got) {
			t.Fatalf("task data/version/token changed: %#v", got)
		}
		st, err := r.Stats()
		if err != nil || st.Events != 1 || st.Tasks != 1 || st.StagedEvents != 1 || st.Checkpoint != pos {
			t.Fatalf("lost journal state: %+v %v", st, err)
		}
		var owner string
		if err = r.Meta("instance_id", &owner); err != nil || owner != "persistent-owner" {
			t.Fatal(owner, err)
		}
		sc, err := r.Schema("orders")
		if err != nil || sc.Generation != "generation" {
			t.Fatal(sc, err)
		}
		ops, err := r.Operations()
		if err != nil || len(ops) != 1 || ops[0].State != "applying" {
			t.Fatal(ops, err)
		}
		if err = r.Ack(got[0].ID); err != nil {
			t.Fatal(err)
		}
		if err = r.Rollback(); err != nil {
			t.Fatal(err)
		}
		pending, _ := r.Events(10)
		if err = r.Transform(pending[0].Sequence, []model.Task{{Table: "later"}}); err != nil {
			t.Fatal(err)
		}
		next, _ := r.Tasks(10)
		if len(next) != 1 || next[0].ID <= got[0].ID {
			t.Fatal("task sequence rolled back")
		}
		r.Close()
	}
	bad := filepath.Join(dir, "bad.db")
	if err = os.WriteFile(bad, []byte("corrupt backup"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = ExportOffline(bad, filepath.Join(dir, "invalid.db"), false); err == nil {
		t.Fatal("accepted corrupt backup")
	}
}

func TestJournalGrowthAndCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	s, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Repeated large bursts must reuse freed pages. Replay indexes deliberately
	// grow with distinct source boundaries and are measured separately below.
	payload := strings.Repeat("x", 16<<10)
	var firstSize int64
	for cycle := range 8 {
		events := make([]model.Event, 128)
		for i := range events {
			events[i] = model.Event{Kind: "insert", Table: "t", Query: payload}
		}
		if err = s.Enqueue(events); err != nil {
			t.Fatal(err)
		}
		pending, _ := s.Events(200)
		ids := make([]uint64, len(pending))
		for i := range pending {
			ids[i] = pending[i].Sequence
		}
		if err = s.TransformBatch(ids, nil); err != nil {
			t.Fatal(err)
		}
		st, _ := s.Stats()
		if cycle == 1 {
			firstSize = st.JournalBytes
		}
		if cycle > 1 && st.JournalBytes > firstSize*2 {
			t.Fatalf("unbounded drained queue growth: %d -> %d", firstSize, st.JournalBytes)
		}
	}
	for i := range 2000 {
		p := model.Position{File: "bin.000001", Pos: uint32(i + 4)}
		if err = s.Append([]model.Event{{Kind: "commit", SourceID: fmt.Sprintf("commit:bin.000001:%d", i+4)}}, p); err != nil {
			t.Fatal(err)
		}
		pending, _ := s.Events(1)
		if err = s.Transform(pending[0].Sequence, nil); err != nil {
			t.Fatal(err)
		}
	}
	st, _ := s.StorageStats()
	if st.HistoryEntries != 4000 || st.HistoryBytes <= 0 || st.ReusableBytes <= 0 {
		t.Fatalf("history and reusable page metrics do not reflect drained journal: %+v", st)
	}
	s.Close()
	compact := path + ".compact"
	if err = ExportOffline(path, compact, true); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(compact)
	if fi.Size() >= st.JournalBytes {
		t.Fatalf("compaction did not reclaim drained pages: %d >= %d", fi.Size(), st.JournalBytes)
	}
	r, err := Open(compact, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, pos := range []uint32{4, 1000, 2003} {
		if _, err = r.SequenceAt(model.Position{File: "bin.000001", Pos: pos}); err != nil {
			t.Fatal("compaction removed replay history", err)
		}
	}
	t.Logf("drained queue high water=%d, source=%d, compact=%d, retained entries=%d history bytes=%d reusable bytes=%d", firstSize, st.JournalBytes, fi.Size(), st.HistoryEntries, st.HistoryBytes, st.ReusableBytes)
}
