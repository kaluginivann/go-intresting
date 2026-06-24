package main

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type Pool struct {
	tasks chan *task
	errs  chan error

	done chan struct{}
	stop chan struct{}

	wg            sync.WaitGroup
	once          sync.Once
	droppedErrors atomic.Uint64
}

func New(cfg *Config) *Pool {
	cfg.validate()
	pool := &Pool{
		tasks: make(chan *task, cfg.QueueSize),
		errs:  make(chan error, cfg.ErrBuf),
		done:  make(chan struct{}),
		stop:  make(chan struct{}),
	}
	pool.wg.Add(cfg.Workers)
	for range cfg.Workers {
		go pool.worker()
	}

	return pool
}

func (p *Pool) Errors() <-chan error {
	return p.errs
}

func (p *Pool) DroppedErrors() uint64 {
	return p.droppedErrors.Load()
}

func (p *Pool) worker() {
	select {
	case <-p.done:
		return
	case t := <-p.tasks:
		p.runTask(t)
	case <-p.stop:
		p.drainAndExit()
		return
	}
}

func (p *Pool) drainAndExit() {
	for {
		select {
		case <-p.done:
			return
		case t := <-p.tasks:
			p.runTask(t)
		default:
			return
		}
	}
}

func (p *Pool) runTask(t *task) {
	defer func() {
		if r := recover(); r != nil {
			p.SendErr(&PanicError{Recovered: r, Stack: debug.Stack()})
		}
	}()
	if err := t.ctx.Err(); err != nil {
		p.SendErr(fmt.Errorf("Error context in task"))
		return
	}
	if err := t.fn(t.ctx); err != nil {
		p.SendErr(fmt.Errorf("Error context in task"))
	}
}

func (p *Pool) Submit(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return NilTasklErr
	}
	select {
	case <-p.stop:
		return ErrPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	t := &task{ctx: ctx, fn: fn}

	select {
	case p.tasks <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stop:
		return ErrPoolClosed
	}
}

func (p *Pool) SendErr(err error) {
	select {
	case p.errs <- err:
		return
	default:
		p.droppedErrors.Add(1)
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	called := false
	p.once.Do(func() {
		called = true
		close(p.stop)
	})
	if !called {
		return ErrAlreadyShutdown
	}
	drained := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		close(p.done)
		<-drained
		return fmt.Errorf("Deadline time exceed")
	}
}
