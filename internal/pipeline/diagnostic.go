package pipeline

import (
	"context"
	"errors"
	"fmt"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	mysql "github.com/go-sql-driver/mysql"
)

// ErrorSummary is safe for ordinary logs: drivers may include row data in Error().
func ErrorSummary(err error) string {
	var ce *ch.Exception
	if errors.As(err, &ce) {
		return fmt.Sprintf("ClickHouse code %d", ce.Code)
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return fmt.Sprintf("MySQL code %d", me.Number)
	}
	if errors.Is(err, context.Canceled) {
		return "context canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "operation deadline exceeded"
	}
	for errors.Unwrap(err) != nil {
		err = errors.Unwrap(err)
	}
	return fmt.Sprintf("%T (details in owner-only status)", err)
}
