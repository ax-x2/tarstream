package pipeline

import (
	"context"
	"io"
)

// ReadStage reads from io.Reader and pushes buffers to output channel.
// first stage in the pipeline, reading from HTTP response or file.
//
// ownership model: buffers are obtained from pool and ownership is transferred
// to the next stage via channel. next stage is responsible for returning buffer to pool.
func ReadStage(
	ctx context.Context,
	reader io.Reader,
	out chan<- []byte,
	errCh chan<- error,
	cache *bufferCache,
) {
	for {
		buf := cache.Get()

		n, err := reader.Read(buf)
		if err == io.EOF {
			cache.Put(buf) // return unused buffer
			return
		}
		if err != nil {
			cache.Put(buf)
			select {
			case errCh <- err:
			case <-ctx.Done():
			}
			return
		}

		// send only filled portion (zero-copy slice)
		select {
		case out <- buf[:n]:
			// buffer ownership transferred to next stage
		case <-ctx.Done():
			cache.Put(buf)
			return
		}
	}
}
