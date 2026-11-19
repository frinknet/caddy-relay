package hub

import "errors"

var (
	errParticipantsCap      = errors.New("hub: participants cap reached")
	errMaxParticipantsCap   = errors.New("hub: max_participants reached")
	errMaxParticipantsPerIP = errors.New("hub: max_participants_per_ip reached")
	errChannelClosed        = errors.New("hub: channel closed")
)
