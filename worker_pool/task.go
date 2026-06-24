package main

import (
	"context"
)

type task struct {
	ctx context.Context
	fn  func(ctx context.Context) error
}
