package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// FileWork represents a file being processed by a worker.
// tar parser creates FileWork and streams chunks to worker via chunkCh.
type FileWork struct {
	meta    *FileMetadata
	chunkCh chan []byte // tar parser sends chunks here (worker reads)
	doneCh  chan error  // worker signals completion (1-buffered)
	fileID  uint64      // for tracking/debugging
}

// NewFileWork creates a new FileWork with the specified metadata and file ID.
func NewFileWork(meta *FileMetadata, fileID uint64, chunkBufferSize int) *FileWork {
	return &FileWork{
		meta:    meta,
		chunkCh: make(chan []byte, chunkBufferSize),
		doneCh:  make(chan error, 1),
		fileID:  fileID,
	}
}

// WorkerPool manages parallel file processing.
// workers process tar entries concurrently, calling FileCallback for each entry.
type WorkerPool struct {
	workerCount int
	workCh      chan *FileWork
	wg          sync.WaitGroup
	callback    FileCallback
	bufferPool  *BufferPool
	ctx         context.Context
	cancel      context.CancelFunc

	// error aggregation (best-effort: collect all errors)
	errors   []error
	errorsMu sync.Mutex
}

// NewWorkerPool creates a worker pool with the specified number of workers.
// each worker gets a dedicated buffer cache (goroutineID = 3 + workerID).
func NewWorkerPool(workerCount int, callback FileCallback, bufferPool *BufferPool) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workerCount: workerCount,
		workCh:      make(chan *FileWork, workerCount*2), // buffer 2x workers
		callback:    callback,
		bufferPool:  bufferPool,
		ctx:         ctx,
		cancel:      cancel,
		errors:      make([]error, 0),
	}
}

// Start launches all worker goroutines.
// workers wait on workCh for FileWork to process.
func (wp *WorkerPool) Start() {
	wp.wg.Add(wp.workerCount)

	for i := 0; i < wp.workerCount; i++ {
		workerID := i

		go func() {
			defer wp.wg.Done()
			wp.worker(workerID)
		}()
	}
}

// worker processes FileWork from workCh.
// lifecycle: OnFileStart → OnFileChunk(s) → OnFileEnd
func (wp *WorkerPool) worker(id int) {
	for {
		select {
		case work, ok := <-wp.workCh:
			if !ok {
				return // channel closed, shutdown
			}

			// process complete file
			err := wp.processFile(work)

			// signal completion to tar parser
			work.doneCh <- err

			// collect error (best-effort)
			if err != nil {
				wp.addError(err)
			}

		case <-wp.ctx.Done():
			return
		}
	}
}

// processFile handles complete file lifecycle: start → chunks → end
func (wp *WorkerPool) processFile(work *FileWork) error {
	// OnFileStart
	action, err := wp.callback.OnFileStart(work.meta)
	if err != nil {
		wp.drainChunks(work.chunkCh) // drain remaining chunks
		return fmt.Errorf("%w: %v", ErrCallback, err)
	}

	if action == ActionSkip {
		wp.drainChunks(work.chunkCh)
		return nil
	}
	if action == ActionStop {
		wp.drainChunks(work.chunkCh)
		wp.cancel() // signal global shutdown
		return nil
	}

	// OnFileChunk for each chunk
	for chunk := range work.chunkCh {
		action, err := wp.callback.OnFileChunk(chunk)
		if err != nil {
			wp.drainChunks(work.chunkCh) // drain remaining
			return fmt.Errorf("%w: %v", ErrCallback, err)
		}

		if action == ActionSkip {
			wp.drainChunks(work.chunkCh)
			break
		}
		if action == ActionStop {
			wp.drainChunks(work.chunkCh)
			wp.cancel()
			return nil
		}
	}

	// OnFileEnd
	action, err = wp.callback.OnFileEnd(work.meta)

	if err != nil {
		return fmt.Errorf("%w: %v", ErrCallback, err)
	}
	if action == ActionStop {
		wp.cancel()
	}

	return nil
}

// drainChunks consumes remaining chunks from channel (for skip/error cases)
func (wp *WorkerPool) drainChunks(chunkCh <-chan []byte) {
	for range chunkCh {
		// discard
	}
}

// Submit sends FileWork to the worker pool.
// blocks if all workers are busy (natural backpressure).
// returns error if context cancelled.
func (wp *WorkerPool) Submit(work *FileWork) error {
	select {
	case wp.workCh <- work:
		return nil
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	}
}

// Shutdown gracefully stops all workers and waits for completion.
// returns aggregated errors from all workers (best-effort collection).
func (wp *WorkerPool) Shutdown() error {
	close(wp.workCh)
	wp.wg.Wait()

	// return first error if any
	wp.errorsMu.Lock()
	defer wp.errorsMu.Unlock()

	if len(wp.errors) > 0 {
		return fmt.Errorf("worker errors: %v", wp.errors)
	}

	return nil
}

// addError appends error to error collection (thread-safe)
func (wp *WorkerPool) addError(err error) {
	wp.errorsMu.Lock()
	defer wp.errorsMu.Unlock()

	wp.errors = append(wp.errors, err)
}
