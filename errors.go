package eventcast

import "errors"

var (
	ErrClosed = errors.New("eventcast: broadcaster is closed")
)
