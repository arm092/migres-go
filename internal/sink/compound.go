package sink

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
)

// PrepareCompoundAlter publishes the name transition first. RenameTask keeps an
// immutable owned-generation plan, so unknown SQL responses and restart cannot
// rename a reincarnated source name or toggle a completed rename back.
func (c *ClickHouse) PrepareCompoundAlter(ctx context.Context, t model.Task, pairs [][2]string, cluster string) (model.Task, error) {
	if len(pairs) != 1 || t.Previous == nil || t.Schema == nil || t.Token == "" || c.Store == nil {
		return t, fmt.Errorf("compound ALTER requires one rename, schemas and durable task identity")
	}
	if pairs[0][0] != t.Table || pairs[0][1] != t.Schema.Table {
		return t, fmt.Errorf("compound ALTER rename/schema mismatch")
	}
	rename := t
	rename.Token += ":table-rename"
	if err := c.RenameTask(ctx, rename, pairs, cluster); err != nil {
		return t, err
	}
	previous := *t.Previous
	previous.Table = pairs[0][1]
	t.Table = pairs[0][1]
	t.Previous = &previous
	sql, err := schema.AlterWithoutRename(t.SQL)
	if err != nil {
		return t, err
	}
	t.SQL = sql
	return t, nil
}

func (c *ClickHouse) CompoundAlter(ctx context.Context, t model.Task, pairs [][2]string, cluster string) error {
	work, err := c.PrepareCompoundAlter(ctx, t, pairs, cluster)
	if err != nil {
		return err
	}
	var absorbed bool
	if err := c.Store.Meta("compoundabsorbed:"+t.Token, &absorbed); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if absorbed {
		return nil
	}
	return c.Alter(ctx, work)
}
