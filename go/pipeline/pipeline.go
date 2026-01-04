package pipeline

import (
	"context"
	"io"
	"sync"

	"github.com/ax-x2/tarstream/go/compression"
)

// pipeline coordinates multi-stage parallel processing:
// HTTP/Reader -> decompress -> tar -> callback
//
// - 3+ concurrent goroutines with buffered channels (configurable capacity)
// - optional 4th goroutine for stream callbacks (async, non-blocking)
// - context-based cancellation propagates through all stages
// - error channel aggregates errors from all stages
// - WaitGroup ensures clean shutdown
type Pipeline struct {
	bufferPool      *BufferPool
	bufferSize      int
	channelCapacity int            // channel buffer capacity (0 = use default: 12)
	streamCallback  StreamCallback // optional callback for decompressed chunks
}

// NewPipeline creates a new pipeline with specified buffer size and channel capacity.
// channelCapacity of 0 uses default (12). higher values increase throughput but use more memory.
func NewPipeline(bufferSize, channelCapacity int) *Pipeline {
	return &Pipeline{
		bufferPool:      NewBufferPool(bufferSize),
		bufferSize:      bufferSize,
		channelCapacity: channelCapacity,
		streamCallback:  nil, // optional, set via SetStreamCallback
	}
}

// SetStreamCallback sets the optional stream callback for decompressed chunks.
// callback runs asynchronously in dedicated goroutine (does not block pipeline).
func (p *Pipeline) SetStreamCallback(callback StreamCallback) {
	p.streamCallback = callback
}

// execute runs the pipeline: reader -> decompress -> tar -> callback
func (p *Pipeline) Execute(
	ctx context.Context,
	reader io.Reader,
	compression compression.CompressionType,
	callback FileCallback,
	maxFileSize *uint64,
) (*ExtractionStats, error) {
	// channel setup (buffered for pipeline efficiency - prevents goroutine starvation)
	capacity := p.channelCapacity
	if capacity == 0 {
		capacity = 12 // default: balances throughput vs memory (12 * bufsize * 2 channels)
	}
	rawCh := make(chan []byte, capacity)      // HTTP -> decompress
	decompCh := make(chan []byte, capacity)   // decompress -> tar
	errCh := make(chan error, 4)              // error aggregation from all stages (increased to 4 for callback goroutine)

	// optional stream callback channel (async, non-blocking)
	var callbackCh chan []byte
	if p.streamCallback != nil {
		callbackCh = make(chan []byte, capacity) // same capacity as other channels
	}

	var wg sync.WaitGroup
	numGoroutines := 2 // HTTP + decompress
	if p.streamCallback != nil {
		numGoroutines++ // +1 for callback goroutine
	}
	wg.Add(numGoroutines)

	// assign dedicated lock-free caches to each goroutine (eliminates lock contention)
	cacheHTTP := p.bufferPool.GetCacheForGoroutine(0)     // http/reader goroutine
	cacheDecomp := p.bufferPool.GetCacheForGoroutine(1)   // decompress goroutine
	cacheTar := p.bufferPool.GetCacheForGoroutine(2)      // tar goroutine

	// HTTP/reader (goroutine)
	go func() {
		defer wg.Done()
		defer close(rawCh)
		ReadStage(ctx, reader, rawCh, errCh, cacheHTTP)
	}()

	// optional stream callback (goroutine) - runs async, does not block pipeline
	if p.streamCallback != nil {
		go func() {
			defer wg.Done()
			for {
				select {
				case chunk, ok := <-callbackCh:
					if !ok {
						return // channel closed
					}
					if err := p.streamCallback.OnDecompressedChunk(chunk); err != nil {
						select {
						case errCh <- err:
						case <-ctx.Done():
						}
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// decompression (goroutine)
	go func() {
		defer wg.Done()
		defer close(decompCh)
		if callbackCh != nil {
			defer close(callbackCh) // close callback channel when done
		}
		DecompressStage(ctx, compression, rawCh, decompCh, errCh, cacheDecomp, callbackCh)
	}()

	// tar parsing stage (runs in this goroutine)
	// running in main goroutine simplifies callback execution (no goroutine leaks from callbacks)
	stats, tarErr := TarStage(ctx, decompCh, callback, maxFileSize, cacheTar)

	// wait for upstream stages to complete
	wg.Wait()
	close(errCh)

	// prioritize tar errors, then upstream errors
	if tarErr != nil {
		return stats, tarErr
	}
	for err := range errCh {
		if err != nil {
			return stats, err
		}
	}

	return stats, nil
}
