package eventcast

type DeliveryPolicy uint8

const (
	Block DeliveryPolicy = iota
	DropLatest
	DropOldest
)
