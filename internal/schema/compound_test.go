package schema

import (
	"strings"
	"testing"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
)

func TestCompoundRenameKeepsColumnSemantics(t *testing.T) {
	c := config.Default()
	old, err := FromCreate("CREATE TABLE before_name(id INT PRIMARY KEY, value INT NOT NULL DEFAULT 1)", "source", "generation", c)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"ALTER TABLE before_name ADD COLUMN extra INT NOT NULL DEFAULT 7, RENAME TO after_name",
		"ALTER TABLE before_name RENAME TO after_name, ADD COLUMN extra INT NOT NULL DEFAULT 7",
	} {
		d, err := Parse(sql)
		if err != nil || d.Kind != "alter" || len(d.Pairs) != 1 || d.Pairs[0] != [2]string{"before_name", "after_name"} {
			t.Fatal(d, err)
		}
		next, rebuild, err := Apply(old, sql, c)
		if err != nil || rebuild || next.Table != "after_name" || next.Generation != old.Generation || len(next.Columns) != 3 {
			t.Fatal(next, rebuild, err)
		}
		plain, err := AlterWithoutRename(sql)
		if err != nil || strings.Contains(plain, "RENAME") {
			t.Fatal(plain, err)
		}
		steps, err := AlterSteps(plain, c)
		if err != nil || len(steps) != 1 || steps[0].Kind != "add" {
			t.Fatal(steps, err)
		}
	}
	if _, err := ReferencesDatabase("ALTER TABLE before_name ADD COLUMN x INT, RENAME TO other.after_name", "source", "source"); err == nil {
		t.Fatal("allowed cross-database compound ALTER")
	}
}

func TestLegacyExpressionDefaultMetadata(t *testing.T) {
	clock, unknown := "CURRENT_TIMESTAMP(6)", "UUID()"
	if got := Default(model.Column{DefaultExpression: true, Default: &clock}); got != "now64(6)" {
		t.Fatalf("legacy clock default: %s", got)
	}
	if got := Default(model.Column{DefaultExpression: true, Default: &unknown}); got != "" {
		t.Fatalf("copied unsafe legacy expression: %s", got)
	}
	if got := Default(model.Column{Default: &clock}); got != Literal(clock) {
		t.Fatalf("literal clock name became expression: %s", got)
	}
}
