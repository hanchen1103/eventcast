package eventcast

type BroadcasterStats struct {
	Closed      bool
	Published   uint64
	Subscribers int
	Dropped     uint64
}

type SubscriptionStats struct {
	ID      uint64
	Buffer  int
	Len     int
	Dropped uint64
	LastSeq uint64
	Closed  bool
}
