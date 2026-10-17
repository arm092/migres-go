package pipeline

import (
	"testing"

	"github.com/arm092/migres-go/internal/model"
)

func TestUpdateKeyEmitsTombstone(t *testing.T) {
	s := model.Schema{Table: "t", Columns: []model.Column{{Name: "tenant"}, {Name: "id"}, {Name: "name"}}, Keys: []string{"tenant", "id"}}
	before := []model.Value{model.Encode(int64(1)), model.Encode(int64(2)), model.Encode("old")}
	after := []model.Value{model.Encode(int64(1)), model.Encode(int64(3)), model.Encode("new")}
	tasks, err := Transform(model.Event{Kind: "update", Table: "t", Sequence: 10, Schema: &s, Rows: []model.Change{{Before: before, After: after}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || len(tasks[0].Rows) != 2 || !tasks[0].Rows[0].Deleted || tasks[0].Rows[1].Deleted {
		t.Fatalf("wrong key update: %+v", tasks)
	}
	if tasks[0].Rows[0].Version != 10 || tasks[0].Rows[1].Version != 10 {
		t.Fatal("versions changed")
	}
}
func TestSnapshotHasBaselineVersion(t *testing.T) {
	s := model.Schema{Table: "t", Columns: []model.Column{{Name: "id"}}, Keys: []string{"id"}}
	tasks, err := Transform(model.Event{Kind: "snapshot", Sequence: 100, Table: "t", Schema: &s, Rows: []model.Change{{After: []model.Value{model.Encode(1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if tasks[0].Rows[0].Version != 1 {
		t.Fatal("snapshot overrides CDC")
	}
}
