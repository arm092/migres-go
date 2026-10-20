package app

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
)

func TestSchemasAtCompoundRename(t *testing.T) {
	s, err := journal.Open(filepath.Join(t.TempDir(), "journal.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old := model.Schema{Table: "before", Generation: "generation", Columns: []model.Column{{Name: "id", CHType: "Int64"}}, Keys: []string{"id"}}
	if err = s.SetMeta("initial_schemas", []model.Schema{old}); err != nil {
		t.Fatal(err)
	}
	fresh := old
	fresh.Table = "after"
	p := model.Position{File: "bin.000001", Pos: 20}
	e := model.Event{Kind: "ddl", Table: "before", Query: "ALTER TABLE before ADD COLUMN extra INT, RENAME TO after", Previous: &old, Schema: &fresh, SourceID: "bin.000001:20", Position: p}
	if err = s.SchemaEvent(e, []model.Schema{fresh}, []string{old.Table}); err != nil {
		t.Fatal(err)
	}
	a := &App{Store: s}
	for _, tc := range []struct {
		pos  uint32
		name string
	}{{4, "before"}, {20, "after"}} {
		got, err := a.schemasAt(model.Position{File: p.File, Pos: tc.pos})
		if err != nil || len(got) != 1 || got[0].Table != tc.name {
			t.Fatalf("position%d: %#v %v", tc.pos, got, err)
		}
	}
}
