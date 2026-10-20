package journal

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/model"
)

func TestRetirementPreservesAbsorbedCompoundNameBarrier(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "journal.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := model.Position{File: "bin.000001", Pos: 20}
	if err = s.Append([]model.Event{{Kind: "ddl"}}, p); err != nil {
		t.Fatal(err)
	}
	events, _ := s.Events(1)
	if err = s.Transform(events[0].Sequence, []model.Task{{Kind: "ddl", Table: "before", SQL: "ALTER TABLE before DROP COLUMN value, ADD COLUMN value INT DEFAULT 7, RENAME TO after", Position: p}}); err != nil {
		t.Fatal(err)
	}
	tasks, _ := s.Tasks(1)
	if err = s.RetireRebuildTasks([]TableWindow{{Table: "before", Through: p}}, p, "earlier-rebuild", true); err != nil {
		t.Fatal(err)
	}
	var absorbed bool
	if err = s.Meta("compoundabsorbed:"+tasks[0].Token, &absorbed); err != nil || !absorbed {
		t.Fatal("missing durable column retirement", absorbed, err)
	}
	remaining, _ := s.Tasks(2)
	if len(remaining) != 1 || remaining[0].Token != tasks[0].Token {
		t.Fatal("lost target rename barrier", remaining)
	}
}
