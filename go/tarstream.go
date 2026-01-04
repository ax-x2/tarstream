package tarstream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ax-x2/tarstream/go/compression"
	"github.com/ax-x2/tarstream/go/pipeline"
)

// types from pipeline for public API
type (
	FileCallback      = pipeline.FileCallback
	StreamCallback    = pipeline.StreamCallback // callback for decompressed chunks (async, non-blocking)
	FileMetadata      = pipeline.FileMetadata
	CallbackAction    = pipeline.CallbackAction
	ExtractionStats   = pipeline.ExtractionStats
	FileTooLargeError = pipeline.FileTooLargeError
)

// constants
const (
	ActionContinue = pipeline.ActionContinue
	ActionSkip     = pipeline.ActionSkip
	ActionStop     = pipeline.ActionStop
)

// errors
var (
	ErrHTTPRequest        = pipeline.ErrHTTPRequest
	ErrIO                 = pipeline.ErrIO
	ErrTarParsing         = pipeline.ErrTarParsing
	ErrDecompression      = pipeline.ErrDecompression
	ErrInvalidCompression = pipeline.ErrInvalidCompression
	ErrURLParse           = pipeline.ErrURLParse
	ErrCallback           = pipeline.ErrCallback
)

// compression types
type CompressionType = compression.CompressionType

const (
	None  = compression.None
	Gzip  = compression.Gzip
	Bzip2 = compression.Bzip2
	Xz    = compression.Xz
	Zstd  = compression.Zstd
	Auto  = compression.Auto
)

// TarStreamExtractor is the main API for streaming tar extraction.
// uses builder pattern for configuration with sensible defaults.
type TarStreamExtractor struct {
	compression     CompressionType
	bufferSize      int
	channelCapacity int
	streamCallback  StreamCallback // optional callback for decompressed chunks
	maxFileSize     *uint64
	httpClient      *http.Client
}

// New creates an extractor with defaults:
// - auto compression detection
// - adaptive buffer size (automatically selected based on compression type)
// - default channel capacity (12, balances throughput vs memory)
// - no file size limit
// - no HTTP timeout (streaming)
func New() *TarStreamExtractor {
	return &TarStreamExtractor{
		compression:     Auto,
		bufferSize:      0, // 0 = adaptive (auto-selected based on compression type)
		channelCapacity: 0, // 0 = use default (12)
		maxFileSize:     nil,
		httpClient:      &http.Client{Timeout: 0}, // no timeout for streaming
	}
}

// WithCompression sets compression type (Auto, None, Gzip, Bzip2, Xz, Zstd)
func (e *TarStreamExtractor) WithCompression(t CompressionType) *TarStreamExtractor {
	e.compression = t
	return e
}

// WithBufferSize sets buffer size for I/O operations.
// setting to 0 enables adaptive sizing (recommended for mixed workloads).
// adaptive sizing automatically selects optimal buffer based on compression type:
//   - gzip: 64KB, zstd: 128KB, bzip2: 256KB, xz: 512KB
func (e *TarStreamExtractor) WithBufferSize(size int) *TarStreamExtractor {
	e.bufferSize = size
	return e
}

// WithChannelCapacity sets the channel buffer capacity for the pipeline.
// higher values increase throughput by reducing goroutine blocking, but use more memory.
// default: 12 (recommended for most workloads)
// memory usage: capacity * bufferSize * 2 channels
//   - example: 12 * 128KB * 2 = 3MB
func (e *TarStreamExtractor) WithChannelCapacity(capacity int) *TarStreamExtractor {
	e.channelCapacity = capacity
	return e
}

// WithStreamCallback sets optional callback for decompressed chunks.
// callback runs asynchronously in dedicated goroutine - does NOT block pipeline.
//
// use cases:
//   - hash/checksum the decompressed tar stream
//   - save raw decompressed tar to disk
//   - implement custom processing on decompressed chunks
//
// chunks are COPIED before callback - safe to retain. callback receives 256KB chunks.
func (e *TarStreamExtractor) WithStreamCallback(callback StreamCallback) *TarStreamExtractor {
	e.streamCallback = callback
	return e
}

// WithMaxFileSize sets maximum file size limit (safety check)
// files exceeding this limit will cause extraction to fail with FileTooLargeError
func (e *TarStreamExtractor) WithMaxFileSize(max uint64) *TarStreamExtractor {
	e.maxFileSize = &max
	return e
}

// WithHTTPClient sets custom HTTP client for ExtractFromURL
// useful for custom timeouts, redirects, proxies, etc.
func (e *TarStreamExtractor) WithHTTPClient(client *http.Client) *TarStreamExtractor {
	e.httpClient = client
	return e
}

// ExtractFromURL downloads and extracts tar archive from URL.
// supports HTTP/HTTPS with streaming - constant memory usage regardless of archive size.
//
// compression is auto-detected from URL extension unless explicitly set.
// context allows cancellation of long-running downloads.
func (e *TarStreamExtractor) ExtractFromURL(
	ctx context.Context,
	url string,
	callback FileCallback,
) (*ExtractionStats, error) {
	start := time.Now()

	// HTTP request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrURLParse, err)
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrHTTPRequest, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrHTTPRequest, resp.StatusCode)
	}

	// auto-detect compression from URL if set to Auto
	comp := e.compression
	if comp == Auto {
		comp = compression.DetectFromURL(url)
		// if still Auto (unknown extension), default to None
		if comp == Auto {
			comp = None
		}
	}

	// adaptive buffer sizing: use compression-aware default if bufferSize is 0
	bufSize := e.bufferSize
	if bufSize == 0 {
		bufSize = compression.CompressionType(comp).OptimalBufferSize()
	}

	// execute pipeline
	p := pipeline.NewPipeline(bufSize, e.channelCapacity)
	if e.streamCallback != nil {
		p.SetStreamCallback(e.streamCallback)
	}
	stats, err := p.Execute(ctx, resp.Body, compression.CompressionType(comp), callback, e.maxFileSize)
	if err != nil {
		return stats, err
	}

	stats.Duration = time.Since(start)
	return stats, nil
}

// ExtractFromReader extracts tar archive from generic io.Reader.
// useful for extracting from files, in-memory buffers, pipes, etc.
//
// compression must be explicitly set (no auto-detection from reader).
func (e *TarStreamExtractor) ExtractFromReader(
	ctx context.Context,
	reader io.Reader,
	callback FileCallback,
) (*ExtractionStats, error) {
	start := time.Now()

	// for readers, compression must be explicitly set (no URL to detect from)
	comp := e.compression
	if comp == Auto {
		comp = None // default to uncompressed
	}

	// adaptive buffer sizing: use compression-aware default if bufferSize is 0
	bufSize := e.bufferSize
	if bufSize == 0 {
		bufSize = compression.CompressionType(comp).OptimalBufferSize()
	}

	p := pipeline.NewPipeline(bufSize, e.channelCapacity)
	if e.streamCallback != nil {
		p.SetStreamCallback(e.streamCallback)
	}
	stats, err := p.Execute(ctx, reader, compression.CompressionType(comp), callback, e.maxFileSize)
	if err != nil {
		return stats, err
	}

	stats.Duration = time.Since(start)
	return stats, nil
}
