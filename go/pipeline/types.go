package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// CallbackAction controls extraction flow
type CallbackAction uint8

const (
	// ActionContinue continues processing the next file/chunk
	ActionContinue CallbackAction = iota
	// ActionSkip skips the current file but continues archive extraction
	// when returned from OnFileStart, this optimization avoids reading tar entry data
	ActionSkip
	// ActionStop halts entire extraction immediately
	ActionStop
)

// FileCallback processes tar entries during streaming extraction.
//
// - OnFileChunk receives a borrowed slice from the buffer pool
// - callbacks *must not* retain references to chunk []byte after return
// - If data needs to be kept, copy it before returning
type FileCallback interface {
	// OnFileStart is called before processing each file
	// returning ActionSkip avoids reading the tar entry data (bandwidth optimization)
	OnFileStart(meta *FileMetadata) (CallbackAction, error)

	// OnFileChunk is called for each data chunk of the current file
	// chunk is a borrowed slice - *must not* be retained after return
	OnFileChunk(chunk []byte) (CallbackAction, error)

	// OnFileEnd is called after processing each file
	OnFileEnd(meta *FileMetadata) (CallbackAction, error)

	// OnError is called when an error occurs during extraction
	// returns action to take (skip to continue, stop to abort)
	// note: no error return - this is for error recovery decisions
	OnError(err error) CallbackAction
}

// FileMetadata describes a tar entry
type FileMetadata struct {
	Path         string
	Size         uint64
	Mode         uint32
	ModifiedTime time.Time
	IsDirectory  bool
}

// clears metadata for reuse in pool
func (m *FileMetadata) Reset() {
	m.Path = ""
	m.Size = 0
	m.Mode = 0
	m.ModifiedTime = time.Time{}
	m.IsDirectory = false
}

// MetadataPool manages reusable FileMetadata structs to reduce allocations
type MetadataPool struct {
	pool sync.Pool
}

// NewMetadataPool creates a new metadata pool
func NewMetadataPool() *MetadataPool {
	return &MetadataPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &FileMetadata{}
			},
		},
	}
}

// get retrieves a FileMetadata from the pool
func (p *MetadataPool) Get() *FileMetadata {
	return p.pool.Get().(*FileMetadata)
}

// put returns a FileMetadata to the pool after resetting it
func (p *MetadataPool) Put(meta *FileMetadata) {
	meta.Reset()
	p.pool.Put(meta)
}

// ExtractionStats tracks metrics from tar extraction
type ExtractionStats struct {
	TotalFiles uint64
	TotalBytes uint64
	Duration   time.Duration
}

// sentinel errors for common error cases
var (
	ErrHTTPRequest        = errors.New("HTTP request failed")
	ErrIO                 = errors.New("IO error")
	ErrTarParsing         = errors.New("tar parsing error")
	ErrDecompression      = errors.New("decompression error")
	ErrInvalidCompression = errors.New("invalid compression format")
	ErrURLParse           = errors.New("URL parse error")
	ErrCallback           = errors.New("callback error")
)

// FileTooLargeError is returned when a file exceeds max size limit
type FileTooLargeError struct {
	Size uint64
	Max  uint64
}

func (e *FileTooLargeError) Error() string {
	return fmt.Sprintf("file too large: %d bytes (max: %d)", e.Size, e.Max)
}
