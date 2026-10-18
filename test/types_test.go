package test

import (
	"context"
	"fmt"
	"github.com/arm092/migres-go/internal/sink"
	"os"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/source"
)

func TestUnsignedEnumSetBinaryJSON(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("types_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	c.Migration.LowCardinality = true
	c.Migration.CDC.DBDebug = true
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	_, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"` (id BIGINT UNSIGNED PRIMARY KEY,status ENUM('red','blue') NULL, tags SET('a','b','c') NOT NULL, flags BIT(8) NOT NULL, payload JSON, raw VARBINARY(8))")
	if err != nil {
		t.Fatal(err)
	}
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	m.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES (18446744073709551615,'red','a,c',b'10101010',JSON_OBJECT('x',1),X'00FF01')")
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	wait(t, ctx, func() bool { return a.Ready.Load() })
	_, err = m.DB.ExecContext(ctx, "UPDATE `"+name+"` SET status='blue',tags='b',flags=b'11111111',payload=JSON_OBJECT('x',2) WHERE id=18446744073709551615")
	if err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var id, flags uint64
		var status, tags, raw, payload string
		err := a.Sink.Conn.QueryRow(ctx, "SELECT id,status,tags,flags,hex(raw),payload FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&id, &status, &tags, &flags, &raw, &payload)
		return err == nil && id == ^uint64(0) && status == "blue" && tags == "b" && flags == 255 && raw == "00FF01" && payload == `{"x":2}`
	})
}

func TestTLSRejectsUnknownCA(t *testing.T) {
	wrong := os.Getenv("MIGRES_TEST_WRONG_CA")
	if wrong == "" {
		t.Skip("requires isolated TLS fixture")
	}
	c := testConfig(t)
	c.MySQL.SSLCA = wrong
	c.MySQL.SSLDisabled = false
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err = m.Check(ctx); err == nil {
		t.Fatal("MySQL accepted unknown CA")
	}
	c.ClickHouse.CACerts = wrong
	c.ClickHouse.Secure = true
	c.ClickHouse.Verify = true
	target, err := sink.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err = target.Check(ctx); err == nil {
		t.Fatal("ClickHouse accepted unknown CA")
	}
}
