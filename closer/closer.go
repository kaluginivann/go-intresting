package closer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type closeFunc func(context.Context) error

type CloserFunc struct {
	name string
	fn   closeFunc
}

type Closer struct {
	funcs []CloserFunc
	mu    sync.Mutex
	once  sync.Once
}

func New() *Closer {
	return &Closer{
		funcs: make([]CloserFunc, 0),
	}
}

func (c *Closer) Add(name string, fn closeFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.funcs = append(c.funcs, CloserFunc{
		name: name,
		fn:   fn,
	})
}

func (c *Closer) CloseAll(ctx context.Context) error {
	var result error

	c.once.Do(func() {
		c.mu.Lock()
		funcs := c.funcs
		c.funcs = nil
		c.mu.Unlock()

		if len(funcs) == 0 {
			return
		}

		var errs []error
		for i := len(funcs) - 1; i >= 0; i-- {
			f := funcs[i]

			start := time.Now()

			slog.Info("close resourse", "name", f.name)

			// TODO: Add timeout functions
			if err := f.fn(ctx); err != nil {
				slog.Error(
					"Error close resourse",
					"name", f.name,
					"error", err,
					"duration", time.Since(start),
				)
				errs = append(errs, err)
			} else {
				slog.Info("resource is closed", "name", f.name)
			}
		}
		result = errors.Join(errs...)
	})
	slog.Info("all resuorce is closed")
	return result
}
