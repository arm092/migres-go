package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"
	_ "time/tzdata"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/source"
)

func main() {
	if err := run(); err != nil {
		slog.Error("migres stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	command := "run"
	args := os.Args[1:]
	if len(args) > 0 && args[0][0] != '-' {
		command = args[0]
		args = args[1:]
	}
	f := flag.NewFlagSet("migres "+command, flag.ContinueOnError)
	path := f.String("config", "config.yml", "configuration file")
	file := f.String("file", "", "binlog file")
	pos := f.Uint("pos", 0, "transaction boundary position")
	gtid := f.String("gtid-set", "", "GTID set")
	discard := f.Bool("discard-pending", false, "archive pending work")
	gap := f.Bool("allow-gap", false, "explicitly permit skipping source changes")
	table := f.String("table", "", "table to resnapshot")
	operation := f.String("operation-id", "", "idempotent operation ID")
	if err := f.Parse(args); err != nil {
		return err
	}
	c, err := config.Load(*path)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if c.Migration.Debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	registerTermination(cancel)
	switch command {
	case "prepare-source":
		m, err := source.Open(c)
		if err != nil {
			return err
		}
		defer m.Close()
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err = m.DB.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+schema.Ref(c.MySQL.Database, c.Migration.SignalTable)+" (id VARCHAR(64) PRIMARY KEY, marker VARCHAR(64) NOT NULL) ENGINE=InnoDB")
		return err
	case "status", "reposition", "reset", "resnapshot":
		req := app.Request{Command: command, File: *file, Pos: uint32(*pos), GTID: *gtid, Discard: *discard, AllowGap: *gap, Table: *table, ID: *operation}
		if *pos > uint(^uint32(0)) {
			return fmt.Errorf("position out of range")
		}
		response, err := app.Control(ctx, c, req)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(response)
	case "run", "check":
		a, err := app.Open(c)
		if err != nil {
			return err
		}
		defer a.Close()
		if command == "check" {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return a.Check(ctx)
		}
		return a.Serve(ctx)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}
