package webui

import "context"

// Radio sends mesh traffic through the active serial broker session.
type Radio interface {
	SendTextMessage(channelIndex int32, to uint32, text string) (uint32, error)
	LocalNodeNum() (uint32, bool)
	FetchCannedMessages(ctx context.Context) ([]string, error)
}
