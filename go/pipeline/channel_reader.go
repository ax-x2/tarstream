package pipeline

import (
	"context"
	"io"
)

// ChannelReader adapts a channel of byte slices to io.Reader interface.
// this bridges the gap between our pipeline channels and archive/tar which expects io.Reader.
//
// zero-copy note: copy() in Read() is unavoidable - archive/tar needs contiguous buffer.
type ChannelReader struct {
	ctx     context.Context
	ch      <-chan []byte
	current []byte // current chunk being read
	offset  int    // read offset in current chunk
	cache   *bufferCache
}

// NewChannelReader creates a new channel reader adapter
func NewChannelReader(ctx context.Context, ch <-chan []byte, cache *bufferCache) *ChannelReader {
	return &ChannelReader{
		ctx:   ctx,
		ch:    ch,
		cache: cache,
	}
}

// read implements io.Reader interface
func (r *ChannelReader) Read(p []byte) (int, error) {
	// if current chunk exhausted, get next from channel
	if r.current == nil || r.offset >= len(r.current) {
		if r.current != nil {
			r.cache.Put(r.current) // return previous chunk to cache
		}

		select {
		case chunk, ok := <-r.ch:
			if !ok {
				return 0, io.EOF // channel closed
			}
			r.current = chunk
			r.offset = 0
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}

	// copy from current chunk
	n := copy(p, r.current[r.offset:])
	r.offset += n
	return n, nil
}
