package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/model"
)

func TestDurableAtomicHandoff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	s, err := Open(path, time.Millisecond*50)
	if err != nil {
		t.Fatal(err)
	}
	ev := model.Event{Kind: "insert", Table: "orders", Rows: []model.Change{{After: []model.Value{{Kind: "int", Text: "7"}}}}}
	if err = s.Append([]model.Event{ev}, model.Position{File: "mysql-bin.000001", Pos: 100}); err != nil {
		t.Fatal(err)
	}
	events, err := s.Events(10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events: %v %v", events, err)
	}
	task := model.Task{Table: "orders", Kind: "insert", Rows: []model.Row{{Values: events[0].Rows[0].After, Version: events[0].Sequence}}}
	if err = s.Transform(events[0].Sequence, []model.Task{task}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, time.Millisecond*50)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	events, _ = s.Events(10)
	tasks, _ := s.Tasks(10)
	if len(events) != 0 || len(tasks) != 1 {
		t.Fatalf("handoff lost: %d events %d tasks", len(events), len(tasks))
	}
	if tasks[0].Rows[0].Version == 0 || tasks[0].Token == "" {
		t.Fatal("unstable task identity")
	}
	if err = s.Ack(tasks[0].ID); err != nil {
		t.Fatal(err)
	}
	tasks, _ = s.Tasks(10)
	if len(tasks) != 0 {
		t.Fatal("ack failed")
	}
}

func TestExclusiveOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	a, err := Open(path, time.Millisecond*20)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(path, time.Millisecond*20)
	if err == nil {
		b.Close()
		t.Fatal("second owner acquired journal")
	}
}

func TestTransformFailureDoesNotLoseEvent(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "journal.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Append([]model.Event{{Kind: "insert", Table: "t"}}, model.Position{})
	if err = s.Transform(999, nil); err == nil {
		t.Fatal("accepted nonexistent event")
	}
	events, _ := s.Events(1)
	if len(events) != 1 {
		t.Fatal("lost pending event")
	}
}

func TestUncommittedTransactionsRemainInvisibleAndReplayStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	s, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	e := model.Event{Kind: "insert", Table: "t", SourceID: "mysql-bin.000001:50", Rows: []model.Change{{}, {}, {}}}
	if err = s.Stage(e); err != nil {
		t.Fatal(err)
	}
	if events, err := s.Events(10); err != nil || len(events) != 0 {
		t.Fatal("uncommitted events visible", events, err)
	}
	s.Close()
	s, err = Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = s.Stage(e); err != nil {
		t.Fatal(err)
	}
	p := model.Position{File: "mysql-bin.000001", Pos: 100}
	if err = s.Commit(p); err != nil {
		t.Fatal(err)
	}
	events, err := s.Events(10)
	if err != nil || len(events) != 2 {
		t.Fatal(events, err)
	}
	first := events[0].Sequence
	if events[1].Sequence <= first+2 {
		t.Fatal("version ranges overlap")
	}
	if err = s.TransformBatch([]uint64{events[0].Sequence, events[1].Sequence}, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.Stage(e); err != nil {
		t.Fatal(err)
	}
	if err = s.Commit(p); err != nil {
		t.Fatal(err)
	}
	events, err = s.Events(10)
	if err != nil || len(events) != 2 || events[0].Sequence != first {
		t.Fatal("replay changed versions", events, err)
	}
}

func TestStagingLimitFailureIsAtomic(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "journal.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxTransactionBytes = 1
	if err = s.StageMany([]model.Event{{Kind: "insert"}, {Kind: "insert"}}); err == nil {
		t.Fatal("oversized transaction accepted")
	}
	st, err := s.Stats()
	if err != nil || st.StagedEvents != 0 {
		t.Fatal("partial staging survived", st, err)
	}
}

func TestCutoverKeepsItsCoordinatorUntilAcknowledgement(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "journal.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := model.Position{File: "mysql-bin.000001", Pos: 100}
	if err = s.Append([]model.Event{{Kind: "ddl", Position: p}}, p); err != nil {
		t.Fatal(err)
	}
	events, _ := s.Events(1)
	if err = s.Transform(events[0].Sequence, []model.Task{{Kind: "ddl", Table: "t", Position: p}, {Kind: "insert", Table: "t", Position: p}, {Kind: "ddl", Table: "t", Position: p, Operation: "next-copy"}}); err != nil {
		t.Fatal(err)
	}
	tasks, _ := s.Tasks(10)
	if err = s.RetireTableTasks("t", p, tasks[0].Token); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.Tasks(10)
	if err != nil || len(remaining) != 2 || remaining[0].ID != tasks[0].ID || remaining[1].Operation != "next-copy" {
		t.Fatal("coordinator lost before durable completion", remaining, err)
	}
}

// Opt-in tmpfs test: an external writer can exhaust disk after the free-space
// check. A failed durable append must preserve the last successful checkpoint.
func TestDiskFault(t *testing.T) {
	root := os.Getenv("MIGRES_DISK_FAULT_DIR")
	if root == "" {
		t.Skip("requires dedicated bounded tmpfs")
	}
	dir, err := os.MkdirTemp(root, "migres-disk-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "journal.db")
	s, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	last := model.Position{File: "mysql-bin.000001", Pos: 4}
	if err = s.Append(nil, last); err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 512*1024)
	failed := false
	for i := 0; i < 100; i++ {
		next := last
		next.Pos++
		err = s.Append([]model.Event{{Kind: "insert", Table: "t", SourceID: fmt.Sprint(i), Rows: []model.Change{{After: []model.Value{model.Encode(payload)}}}}}, next)
		if err != nil {
			failed = true
			break
		}
		last = next
	}
	if !failed {
		t.Fatal("bounded disk did not fill")
	}
	t.Logf("expected bounded-disk error: %v", err)
	s.Close()
	s, err = Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var current model.Position
	if err = s.Meta("checkpoint", &current); err != nil || current != last {
		t.Fatalf("checkpoint lost: %+v want %+v err=%v", current, last, err)
	}
	events, err := s.Events(100)
	if err != nil || len(events) != int(last.Pos-4) {
		t.Fatalf("committed events lost %d want %d: %v", len(events), last.Pos-4, err)
	}
}
