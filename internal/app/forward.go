package app

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/pipeline"
	"github.com/arm092/migres-go/internal/schema"
)

// A gap may omit rows, but cannot silently omit schema history. Conservatively
// reject SQL-bearing transactions in the unconsumed interval. ROW DML has only
// BEGIN/COMMIT query events and remains available with explicit allow-gap.
func (a *App) validateForwardSchema(ctx context.Context, from, to model.Position) error {
	logs, err := a.Source.DB.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return err
	}
	cols, err := logs.Columns()
	if err != nil {
		logs.Close()
		return err
	}
	var files []string
	for logs.Next() {
		values := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err = logs.Scan(dest...); err != nil {
			logs.Close()
			return err
		}
		files = append(files, string(values[0]))
	}
	err = logs.Err()
	logs.Close()
	if err != nil {
		return err
	}
	started, reached := false, false
	for _, file := range files {
		if file == from.File {
			started = true
		}
		if !started {
			continue
		}
		position := uint32(4)
		if file == from.File {
			position = from.Pos
		}
		for {
			if file == to.File && position >= to.Pos {
				reached = true
				break
			}
			rows, e := a.Source.DB.QueryContext(ctx, "SHOW BINLOG EVENTS IN "+schema.Literal(file)+" FROM "+fmt.Sprint(position)+" LIMIT 256")
			if e != nil {
				return e
			}
			count := 0
			for rows.Next() {
				var eventFile, kind, info string
				var start, id, end uint64
				if e = rows.Scan(&eventFile, &start, &kind, &id, &end, &info); e != nil {
					break
				}
				if file == to.File && start >= uint64(to.Pos) {
					reached = true
					break
				}
				if kind == "Query" {
					q := strings.ToUpper(strings.TrimSpace(info))
					if q != "BEGIN" && q != "COMMIT" && q != "ROLLBACK" {
						e = fmt.Errorf("forward gap contains SQL without retained schema history; use resnapshot")
						break
					}
				}
				if end <= uint64(position) {
					e = fmt.Errorf("binlog scan did not advance")
					break
				}
				position = uint32(end)
				count++
			}
			if e == nil {
				e = rows.Err()
			}
			rows.Close()
			if e != nil {
				return e
			}
			if reached || count == 0 {
				break
			}
		}
		if file == to.File {
			break
		}
		if !pipeline.Reached(to, model.Position{File: file, Pos: position}) {
			break
		}
	}
	if !reached {
		return fmt.Errorf("forward gap binlog history is unavailable")
	}
	return nil
}
