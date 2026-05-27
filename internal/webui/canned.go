package webui

import (
	"context"
	"errors"
)

var ErrRadioUnavailable = errors.New("radio not connected")

func (h *Hub) FetchCanned(ctx context.Context) ([]string, error) {
	r := h.radio()
	if r == nil {
		return nil, ErrRadioUnavailable
	}
	return r.FetchCannedMessages(ctx)
}
