package pipeline

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
)

// TarStage parses tar format and invokes callbacks for each file.
// final stage running in the main goroutine (simplifies callback execution).
//
// - metadata pooling: Reuse FileMetadata structs
// - single buffer reuse: one chunk buffer for all chunks of current file
// - skip optimization: ActionSkip on OnFileStart avoids reading tar entry data
func TarStage(
	ctx context.Context,
	in <-chan []byte,
	callback FileCallback,
	maxFileSize *uint64,
	cache *bufferCache,
) (*ExtractionStats, error) {
	// create channel reader adapter (chan []byte → io.Reader)
	reader := NewChannelReader(ctx, in, cache)
	tarReader := tar.NewReader(reader)

	stats := &ExtractionStats{}
	metaPool := NewMetadataPool()

	// single chunk buffer reused for all files
	chunkBuf := cache.Get()
	defer cache.Put(chunkBuf)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, fmt.Errorf("%w: %v", ErrTarParsing, err)
		}

		// get pooled metadata
		meta := metaPool.Get()
		meta.Path = header.Name
		meta.Size = uint64(header.Size)
		meta.Mode = uint32(header.Mode)
		meta.ModifiedTime = header.ModTime
		meta.IsDirectory = header.Typeflag == tar.TypeDir

		// check max file size limit
		if maxFileSize != nil && meta.Size > *maxFileSize {
			metaPool.Put(meta)
			return stats, &FileTooLargeError{Size: meta.Size, Max: *maxFileSize}
		}

		// callback: OnFileStart
		action, err := callback.OnFileStart(meta)
		if err != nil {
			metaPool.Put(meta)
			return stats, fmt.Errorf("%w: %v", ErrCallback, err)
		}

		switch action {
		case ActionSkip:
			stats.TotalFiles++
			metaPool.Put(meta)
			continue // skip reading file data (optimization)
		case ActionStop:
			metaPool.Put(meta)
			return stats, nil
		}

		// read file data (if not directory)
		if !meta.IsDirectory {
			for {
				n, readErr := tarReader.Read(chunkBuf)
				if n > 0 {
					stats.TotalBytes += uint64(n)

					// callback: OnFileChunk (zero-copy slice - do not retain)
					action, err := callback.OnFileChunk(chunkBuf[:n])
					if err != nil {
						metaPool.Put(meta)
						return stats, fmt.Errorf("%w: %v", ErrCallback, err)
					}

					if action == ActionSkip {
						// skip rest of file but continue archive
						// drain remaining bytes
						io.Copy(io.Discard, tarReader)
						break
					} else if action == ActionStop {
						metaPool.Put(meta)
						return stats, nil
					}
				}

				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					metaPool.Put(meta)
					return stats, readErr
				}
			}
		}

		// callback: OnFileEnd
		action, err = callback.OnFileEnd(meta)
		metaPool.Put(meta) // return to pool

		if err != nil {
			return stats, fmt.Errorf("%w: %v", ErrCallback, err)
		}
		if action == ActionStop {
			return stats, nil
		}

		stats.TotalFiles++
	}

	return stats, nil
}

// TarStageParallel parses tar format and dispatches files to worker pool for parallel processing.
// streaming chunk architecture: chunks are sent to workers via channels (no full file buffering).
//
// - workers process OnFileStart/OnFileChunk/OnFileEnd in parallel
// - memory bounded: max N files in-flight (N = worker pool size)
// - tar parsing remains sequential (archive/tar is not thread-safe)
func TarStageParallel(
	ctx context.Context,
	in <-chan []byte,
	callback FileCallback,
	maxFileSize *uint64,
	cache *bufferCache,
	workerPool *WorkerPool,
) (*ExtractionStats, error) {
	// create channel reader adapter (chan []byte → io.Reader)
	reader := NewChannelReader(ctx, in, cache)
	tarReader := tar.NewReader(reader)

	stats := &ExtractionStats{}
	metaPool := NewMetadataPool()

	// single chunk buffer for reading (reused across all files)
	chunkBuf := cache.Get()
	defer cache.Put(chunkBuf)

	var fileID uint64 = 0

	for {
		// read tar header
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, fmt.Errorf("%w: %v", ErrTarParsing, err)
		}

		fileID++

		// get pooled metadata
		meta := metaPool.Get()
		meta.Path = header.Name
		meta.Size = uint64(header.Size)
		meta.Mode = uint32(header.Mode)
		meta.ModifiedTime = header.ModTime
		meta.IsDirectory = header.Typeflag == tar.TypeDir

		// check file size limit
		if maxFileSize != nil && meta.Size > *maxFileSize {
			metaPool.Put(meta)
			return stats, &FileTooLargeError{Size: meta.Size, Max: *maxFileSize}
		}

		// create FileWork (8 chunk buffer per file)
		work := NewFileWork(meta, fileID, 8)

		// submit to worker pool (blocks if all workers busy - natural backpressure)
		if err := workerPool.Submit(work); err != nil {
			metaPool.Put(meta)
			close(work.chunkCh)
			return stats, err
		}

		// stream chunks to worker
		var totalBytes uint64
		if !meta.IsDirectory {
			for {
				n, readErr := tarReader.Read(chunkBuf)
				if n > 0 {
					totalBytes += uint64(n)

					// COPY chunk (worker owns it)
					chunkCopy := make([]byte, n)
					copy(chunkCopy, chunkBuf[:n])

					// send to worker
					select {
					case work.chunkCh <- chunkCopy:
					case <-ctx.Done():
						close(work.chunkCh)
						return stats, ctx.Err()
					}
				}

				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					close(work.chunkCh)
					return stats, readErr
				}
			}
		}

		// signal EOF to worker
		close(work.chunkCh)

		// wait for worker completion
		select {
		case err := <-work.doneCh:
			// return metadata to pool (worker is done with it)
			metaPool.Put(meta)

			if err != nil {
				// worker error (best-effort: collect but continue)
				// errors are aggregated in workerPool.Shutdown()
			}
		case <-ctx.Done():
			metaPool.Put(meta)
			return stats, ctx.Err()
		}

		stats.TotalFiles++
		stats.TotalBytes += totalBytes
	}

	return stats, nil
}
