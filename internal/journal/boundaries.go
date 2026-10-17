package journal

import "github.com/arm092/migres-go/internal/model"

func (s *Store) Boundaries() ([]model.Position, error) {
	return read[model.Position](s, "boundaries", 0)
}
