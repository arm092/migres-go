package app

import (
	"fmt"

	"github.com/arm092/migres-go/internal/model"
	gm "github.com/go-mysql-org/go-mysql/mysql"
)

func (a *App) resolveGTID(raw string) (model.Position, error) {
	set, err := gm.ParseGTIDSet("mysql", raw)
	if err != nil {
		return model.Position{}, err
	}
	boundaries, err := a.Store.Boundaries()
	if err != nil {
		return model.Position{}, err
	}
	for _, p := range boundaries {
		if p.GTID == "" {
			continue
		}
		known, e := gm.ParseGTIDSet("mysql", p.GTID)
		if e == nil && known.Equal(set) {
			return p, nil
		}
	}
	return model.Position{}, fmt.Errorf("GTID set is not an exact retained transaction boundary; use file/position or resnapshot")
}
