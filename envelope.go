package eventcast

import "time"

type Envelope[T any] struct {
	Seq       uint64
	Published time.Time
	Event     T
}
