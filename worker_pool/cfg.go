package main

type Config struct {
	Workers   int
	QueueSize int
	ErrBuf    int
}

func (c Config) validate() {
	if c.Workers <= 0 {
		panic("workers must be more than 0")
	}
	if c.QueueSize < 0 {
		panic("queue size must be more than or equal 0")
	}
	if c.ErrBuf < 0 {
		panic("error buffer must be more than or equal 0")
	}
}
