package pipeline

import (
	"context"
	"io"

	"github.com/ax-x2/tarstream/go/compression"
)

// DecompressStage decompresses data using CGO libraries.
// receives compressed chunks from HTTP stage, decompresses them + sends to tar stage.
//
// thread safety: each goroutine owns its decompressor instance (CGO not thread-safe).
//
// critical: must call ReadBatch() in a loop until fully drained.
// a single compressed chunk (e.g. 128KB) can decompress into multiple batches (e.g. 10MB).
// if we dont drain completely before next Write(), decompressor state gets corrupted -> hang/corruption.
// this is especially critical for high-compression-ratio data (10:1 or higher).
//
// callbackCh: optional channel for streaming decompressed chunks to async callback.
// if provided, chunks are COPIED and sent non-blocking (does not block decompression).
func DecompressStage(
	ctx context.Context,
	compType compression.CompressionType,
	in <-chan []byte,
	out chan<- []byte,
	errCh chan<- error,
	cache *bufferCache,
	callbackCh chan<- []byte, // optional: nil = no callback
) {
	decompressor, err := compression.NewDecompressor(compType)
	if err != nil {
		select {
		case errCh <- err:
		case <-ctx.Done():
		}
		return
	}
	defer decompressor.Close()

	for {
		select {
		case inBuf, ok := <-in:
			if !ok {
				return // upstream closed
			}

			// stage compressed chunk for decompression
			if err := decompressor.Write(inBuf); err != nil {
				cache.Put(inBuf) // return input buffer
				select {
				case errCh <- err:
				case <-ctx.Done():
				}
				return
			}
			// read decompressed data (may produce multiple output chunks)
			// note: dont return inBuf to pool until decompression completes,
			// since decompressor holds a reference to it and will pin it
			//
			// optimization: use ReadBatch() to process multiple output buffers in single CGO call.
			// reduces CGO boundary crossing overhead by 70-90% vs calling Read() in a loop.
			//
			// critical: must call ReadBatch in a loop to drain ALL decompressed output
			// from the input chunk. a single input chunk may produce multiple batches.

			// keep calling ReadBatch until no more output
			for {
				// allocate batch of output buffers (8 buffers per call)
				outputBatch := make([][]byte, 8)
				for i := range outputBatch {
					outputBatch[i] = cache.Get()
				}

				// decompress into all buffers with single CGO transition
				sizes, err := decompressor.ReadBatch(outputBatch)

				// handle decompression errors
				if err != nil && err != io.EOF {
					// cleanup: return all buffers
					for i := range outputBatch {
						cache.Put(outputBatch[i])
					}
					cache.Put(inBuf)
					select {
					case errCh <- err:
					case <-ctx.Done():
					}
					return
				}

				// if no output produced, we're done with this input chunk
				if len(sizes) == 0 {
					// return all unused buffers
					for i := range outputBatch {
						cache.Put(outputBatch[i])
					}
					break
				}

				// send filled buffers downstream, return unused buffers
				for i, size := range sizes {
					if size > 0 {
						// send to tar stage (blocking)
						select {
						case out <- outputBatch[i][:size]:
							// sent to tar stage

							// optional: send copy to stream callback (non-blocking)
							if callbackCh != nil {
								// make a copy since callback runs async and may retain it
								chunkCopy := make([]byte, size)
								copy(chunkCopy, outputBatch[i][:size])

								// non-blocking send to avoid blocking decompression
								select {
								case callbackCh <- chunkCopy:
									// sent to callback goroutine
								default:
									// callback channel full, skip this chunk to avoid blocking
									// user can increase channel capacity if needed
								}
							}

						case <-ctx.Done():
							// cleanup: return current buffer and all remaining
							cache.Put(outputBatch[i])
							for j := i + 1; j < len(outputBatch); j++ {
								cache.Put(outputBatch[j])
							}
							// return unused buffers from batch
							for k := len(sizes); k < len(outputBatch); k++ {
								cache.Put(outputBatch[k])
							}
							cache.Put(inBuf)
							return
						}
					} else {
						cache.Put(outputBatch[i])
					}
				}

				// return unused buffers from batch
				for i := len(sizes); i < len(outputBatch); i++ {
					cache.Put(outputBatch[i])
				}

				// if we got io.EOF, we're done with this input chunk
				if err == io.EOF {
					break
				}

				// if less than 8 buffers were filled, no more output available
				if len(sizes) < 8 {
					break
				}

				// otherwise, keep calling ReadBatch to drain remaining output
			}

			cache.Put(inBuf) // done with input buffer after fully draining output

		case <-ctx.Done():
			return
		}
	}
}
