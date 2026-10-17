package app

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
)

func TestForwardGapRejectsMissingDDLHistory(t *testing.T) {
	if os.Getenv("MIGRES_INTEGRATION") != "1" {
		t.Skip("isolated fixture required")
	}
	c := config.Default()
	c.MySQL.Port = 33080
	c.MySQL.User = "root"
	c.MySQL.Password = "test-only"
	c.MySQL.Database = "migres_src"
	c.ClickHouse.Port = 39000
	c.ClickHouse.Password = "test-only"
	c.ClickHouse.Database = "migres_dst"
	c.DataDir = t.TempDir()
	if host := os.Getenv("MIGRES_TEST_HOST"); host != "" {
		c.MySQL.Host = host
		c.ClickHouse.Host = host
	}
	name := fmt.Sprintf("forward_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.Source.DB.ExecContext(ctx, "CREATE TABLE `"+name+"`(id BIGINT PRIMARY KEY,value INT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	defer a.Source.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	if err = a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		owned, _ := a.Store.Owned()
		for _, s := range owned {
			a.Sink.DropOwned(context.Background(), s)
		}
	}()
	if _, err = a.Source.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(1,10)"); err != nil {
		t.Fatal(err)
	}
	p, err := a.Source.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := Request{Command: "reposition", File: p.File, Pos: p.Pos, AllowGap: true}
	if err = a.validateRequest(ctx, r); err != nil {
		t.Fatalf("safe ROW gap rejected: %v", err)
	}
	r.AllowGap = false
	if err = a.validateRequest(ctx, r); err == nil {
		t.Fatal("gap accepted without explicit allow-gap")
	}
	if _, err = a.Source.DB.ExecContext(ctx, "ALTER TABLE `"+name+"` RENAME COLUMN value TO renamed"); err != nil {
		t.Fatal(err)
	}
	p, err = a.Source.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r = Request{Command: "reposition", File: p.File, Pos: p.Pos, AllowGap: true}
	if err = a.validateRequest(ctx, r); err == nil {
		t.Fatal("gap silently omitted schema history")
	}
	var cp model.Position
	if err = a.Store.Meta("checkpoint", &cp); err != nil || cp.Pos == p.Pos {
		t.Fatal("validation mutated checkpoint", cp, err)
	}
}
