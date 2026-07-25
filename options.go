package eventcast

const defaultBufferSize = 16

type subscribeOptions struct {
	buffer int
	policy DeliveryPolicy
}

type SubscribeOption func(*subscribeOptions)

func WithBuffer(size int) SubscribeOption {
	return func(o *subscribeOptions) {
		if size >= 0 {
			o.buffer = size
		}
	}
}

func WithPolicy(policy DeliveryPolicy) SubscribeOption {
	return func(o *subscribeOptions) {
		o.policy = policy
	}
}

func defaultSubscribeOptions() subscribeOptions {
	return subscribeOptions{
		buffer: defaultBufferSize,
		policy: Block,
	}
}
