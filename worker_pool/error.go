package main

import (
	"errors"
	"fmt"
)

var (
	NilTasklErr        error = errors.New("Nil task error")
	ErrPoolClosed      error = errors.New("worker pool is closed")
	ErrAlreadyShutdown error = errors.New("shutdown is already call")
)

type PanicError struct {
	Recovered any
	Stack     []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("job is panic %v", e.Recovered)
}
