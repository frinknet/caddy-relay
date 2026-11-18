package hub

import "errors"

var (
	errParticipantsCap    = errors.New("hub: participants cap reached")
	errMaxParticipantsCap = errors.New("hub: max_participants reached")
	errChannelClosed      = errors.New("hub: channel closed")
)
