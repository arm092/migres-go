package app

import (
	"errors"
	"fmt"
	"os"

	"github.com/arm092/migres-go/internal/model"
)

func (a *App) resolveGTID(raw string) (model.Position, error) {
	p, err := a.Store.GTIDBoundary(raw)
	if errors.Is(err, os.ErrNotExist) {
		return p, fmt.Errorf("GTID set is not an exact retained transaction boundary; use file/position or resnapshot")
	}
	return p, err
}
