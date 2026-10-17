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

func TestKeyChangesPreserveEnforcedIdentity(t *testing.T) {
	c := config.Default()
	s, err := FromCreate("CREATE TABLE t(id BIGINT PRIMARY KEY, replacement BIGINT)", "db", "gen", c)
	if err != nil {
		t.Fatal(err)
	}
	next, rebuild, err := Apply(s, "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY(replacement)", c)
	if err != nil || !rebuild || len(next.Keys) != 1 || next.Keys[0] != "replacement" || next.Columns[1].Nullable || next.Columns[1].CHType != "Int64" {
		t.Fatalf("new PK was not normalized: %+v %v", next, err)
	}
	c.MySQL.Keys["t"] = []string{"id"}
	for _, query := range []string{"ALTER TABLE t DROP INDEX selected_unique", "ALTER TABLE t MODIFY COLUMN id BIGINT NULL", "ALTER TABLE t DROP COLUMN id"} {
		if _, _, err = Apply(s, query, c); err == nil {
			t.Fatalf("unsafe identity change accepted: %s", query)
		}
	}
	next, _, err = Apply(s, "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY(replacement)", c)
	if err != nil || next.Keys[0] != "id" {
		t.Fatalf("configured identity changed: %+v %v", next, err)
	}
}

func TestHistoricalCharsets(t *testing.T) {
	c := config.Default()
	s, err := FromCreate("CREATE TABLE t (id VARCHAR(10) PRIMARY KEY,v TEXT,b BLOB) DEFAULT CHARSET=latin1", "db", "g", c)
	if err != nil {
		t.Fatal(err)
	}
	if s.Charset != "latin1" || s.Columns[0].Charset != "latin1" || s.Columns[2].Charset != "binary" {
		t.Fatalf("%+v", s)
	}
	next, rebuild, err := Apply(s, "ALTER TABLE t DEFAULT CHARSET=utf8mb4, ADD COLUMN x VARCHAR(10)", c)
	if err != nil {
		t.Fatal(err)
	}
	if rebuild || next.Columns[0].Charset != "latin1" || next.Columns[3].Charset != "utf8mb4" {
		t.Fatalf("%+v rebuild=%v", next, rebuild)
	}
	next, rebuild, err = Apply(next, "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4", c)
	if err != nil {
		t.Fatal(err)
	}
	if !rebuild || next.Columns[0].Charset != "utf8mb4" || next.Columns[1].Charset != "utf8mb4" {
		t.Fatalf("%+v", next)
	}
	s, err = FromCreate("CREATE TABLE t (id INT PRIMARY KEY,v VARCHAR(20))", "db", "g", c, "latin1")
	if err != nil || s.Columns[1].Charset != "latin1" {
		t.Fatalf("%+v %v", s, err)
	}
	for _, q := range []string{"ALTER DATABASE db CHARACTER SET utf8mb4", "DROP DATABASE db"} {
		found, err := ReferencesDatabase(q, "", "db")
		if err != nil || !found {
			t.Fatalf("database DDL ignored %s %v", q, err)
		}
	}
}

func TestColumnCollationDeterminesHistoricalCharset(t *testing.T) {
	c := config.Default()
	s, err := FromCreate("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(10) COLLATE latin1_bin) DEFAULT CHARSET=utf8mb4", "db", "g", c)
	if err != nil || s.Columns[1].Charset != "latin1" {
		t.Fatalf("%+v %v", s, err)
	}
	next, _, err := Apply(s, "ALTER TABLE t ADD COLUMN x VARCHAR(10) COLLATE latin1_bin", c)
	if err != nil || next.Columns[2].Charset != "latin1" {
		t.Fatalf("%+v %v", next, err)
	}
	next, _, err = Apply(s, "ALTER TABLE t ADD COLUMN x VARCHAR(10), DEFAULT CHARSET=latin1", c)
	if err != nil || next.Columns[2].Charset != "latin1" {
		t.Fatalf("%+v %v", next, err)
	}
}
