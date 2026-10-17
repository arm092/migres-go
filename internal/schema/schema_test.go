package schema

import (
	"strings"
	"testing"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
)

func TestDDLUsesFullPrimaryKey(t *testing.T) {
	s := model.Schema{Table: "orders", Generation: "fixed", Columns: []model.Column{{Name: "tenant", CHType: "UInt64"}, {Name: "id", CHType: "UInt64"}}, Keys: []string{"tenant", "id"}}
	c := config.Default()
	c.ClickHouse.Database = "target"
	sql, err := Create(s, c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "ORDER BY (`tenant`, `id`)") {
		t.Fatal(sql)
	}
	c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
	c.ClickHouse.Cluster = "analytics"
	sql, err = Create(s, c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "/fixed") || !strings.Contains(sql, "ON CLUSTER `analytics`") {
		t.Fatal(sql)
	}
}
func TestMapTypes(t *testing.T) {
	for _, tc := range []struct{ source, want string }{{"bigint unsigned", "UInt64"}, {"decimal(30,8)", "Decimal(30,8)"}, {"varbinary(10)", "String"}, {"datetime(6)", "DateTime64(6, 'UTC')"}} {
		got, err := Type(tc.source, false, "UTC", false)
		if err != nil || got != tc.want {
			t.Fatalf("%s: %s %v", tc.source, got, err)
		}
	}
}
func TestDDLAST(t *testing.T) {
	for _, q := range []string{"ALTER TABLE `orders` ADD COLUMN x INT, DROP COLUMN y", "RENAME TABLE a TO b, c TO d", "TRUNCATE TABLE a", "ALTER TABLE db.a RENAME TO b"} {
		d, err := Parse(q)
		if err != nil || len(d.Tables) == 0 {
			t.Fatalf("%s: %+v %v", q, d, err)
		}
	}
}

func TestQualifiedDDLDatabaseScope(t *testing.T) {
	for _, tc := range []struct {
		sql, defaultDB string
		want, fail     bool
	}{
		{"ALTER TABLE source.orders ADD COLUMN x INT", "other", true, false},
		{"DROP TABLE other.orders", "source", false, false},
		{"DROP TABLE source.orders,other.orders", "other", true, true},
		{"DROP TABLE other.orders,orders", "source", true, true},
	} {
		found, err := ReferencesDatabase(tc.sql, tc.defaultDB, "source")
		if found != tc.want || (err != nil) != tc.fail {
			t.Fatalf("%s: %v %v", tc.sql, found, err)
		}
	}
}
