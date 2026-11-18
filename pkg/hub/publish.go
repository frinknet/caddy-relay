package hub

// Reasons a channel did not deliver a published message. Status.Reason carries
// one of these and is empty when nothing was lost.
const (
	// ReasonOversize means the message exceeded the channel's max_size and was
	// discarded instead of retained.
	ReasonOversize = "oversize"
	// ReasonSlowSubscriber means the message reached the channel but at least
	// one subscriber's queue was full, so that subscriber missed it.
	ReasonSlowSubscriber = "slow subscriber"
	// ReasonMemory means max_memory left no room for the message, even after
	// evicting idle channels.
	ReasonMemory = "memory"
	// ReasonClosed means the channel stopped before handling the message.
	ReasonClosed = "closed"
	// ReasonUnavailable means the channel could not be created: no rule matched
	// the name, or max_channels was reached.
	ReasonUnavailable = "unavailable"
)

// Status is what happened to one channel's copy of a published message.
type Status struct {
	// Enqueued reports whether the channel accepted the message into its ring
	// and handed it to every subscriber that had room.
	Enqueued bool
	// Dropped counts the subscribers whose queue was full, so they did not
	// receive this message. It is 0 when Enqueued is false, because then the
	// whole channel missed it.
	Dropped int
	// Reason is empty when nothing was lost; otherwise one of the Reason
	// constants.
	Reason string
}

// OK reports whether the channel took the message without dropping it.
func (s Status) OK() bool { return s.Enqueued && s.Dropped == 0 }

// String prints like the bool this replaced, so a template that indexes a
// Result keeps its output.
func (s Status) String() string {
	if s.Enqueued {
		return "true"
	}
	return "false"
}

// Result is the return of Hub.Publish. It reports, per channel name, what
// happened to the message. Its String() is empty so bare use in a template
// emits nothing — use range or index to inspect.
type Result map[string]Status

// String returns "" so a bare {{ ... | publish ... }} emits nothing.
func (Result) String() string { return "" }

// OK reports whether every channel took the message without dropping it. An
// empty result is OK: there was nothing to publish to.
func (r Result) OK() bool {
	for _, st := range r {
		if !st.OK() {
			return false
		}
	}
	return true
}

// Enqueued reports whether the named channel accepted the message.
func (r Result) Enqueued(channel string) bool { return r[channel].Enqueued }

// Dropped returns the total number of subscribers that missed the message
// across every channel in the result.
func (r Result) Dropped() int {
	total := 0
	for _, st := range r {
		total += st.Dropped
	}
	return total
}
