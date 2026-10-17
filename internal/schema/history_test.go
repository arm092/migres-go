package schema

import (
	"testing"

	"github.com/arm092/migres-go/internal/config"
)

func TestChangeColumnRetainsOrdinalPosition(t *testing.T) {
	c := config.Default()
	s, err := FromCreate("CREATE TABLE t (id BIGINT PRIMARY KEY, value VARCHAR(20), extra INT NOT NULL DEFAULT 4)", "source", "gen", c)
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := Apply(s, "ALTER TABLE t CHANGE COLUMN value label VARCHAR(20)", c)
	if err != nil {
		t.Fatal(err)
	}
	if next.Columns[1].Name != "label" || next.Columns[2].Name != "extra" {
		t.Fatal("column order changed")
	}
}
func TestDropAddIsNotRename(t *testing.T) {
	steps, err := AlterSteps("ALTER TABLE t DROP COLUMN a, ADD COLUMN b INT DEFAULT 5", config.Default())
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[0].Kind != "drop" || steps[1].Kind != "add" {
		t.Fatalf("wrong semantics: %+v", steps)
	}
}
func TestCustomKeyRequiresConstraint(t *testing.T) {
	c := config.Default()
	c.MySQL.Keys["t"] = []string{"name"}
	if _, err := FromCreate("CREATE TABLE t (id INT PRIMARY KEY,name VARCHAR(20) NOT NULL)", "db", "gen", c); err == nil {
		t.Fatal("unconstrained key accepted")
	}
	if _, err := FromCreate("CREATE TABLE t (name VARCHAR(20) NOT NULL UNIQUE)", "db", "gen", c); err != nil {
		t.Fatal(err)
	}
}
