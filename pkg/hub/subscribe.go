package hub

import "errors"

var (
	errParticipantsCap = errors.New("hub: participants cap reached")
	errChannelClosed   = errors.New("hub: channel closed")
)