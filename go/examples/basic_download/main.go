package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	tarstream "github.com/ax-x2/tarstream/go"
)

// DecompressedStreamBuffer stores decompressed tar stream in memory.
// runs asynchronously in dedicated goroutine - does NOT block pipeline.
//
// example use case: keep decompressed tar in memory for later re-processing,
// verification, or saving to disk without re-downloading.
type DecompressedStreamBuffer struct {
	chunks      [][]byte      // decompressed chunks stored in memory
	totalBytes  atomic.Uint64 // total bytes stored
	mu          sync.Mutex    // protects chunks slice
	maxSize     uint64        // optional size limit (0 = unlimited)
	enabled     bool          // enable/disable buffering
}

// NewDecompressedStreamBuffer creates a new in-memory buffer for decompressed stream.
// maxSize: maximum bytes to buffer (0 = unlimited, be careful with large archives!)
func NewDecompressedStreamBuffer(maxSize uint64, enabled bool) *DecompressedStreamBuffer {
	return &DecompressedStreamBuffer{
		chunks:  make([][]byte, 0, 1024), // pre-allocate for 1024 chunks
		maxSize: maxSize,
		enabled: enabled,
	}
}

// OnDecompressedChunk stores each decompressed chunk in memory (async, non-blocking)
func (b *DecompressedStreamBuffer) OnDecompressedChunk(chunk []byte) error {
	if !b.enabled {
		return nil // buffering disabled
	}

	// check size limit
	if b.maxSize > 0 && b.totalBytes.Load() >= b.maxSize {
		return nil // size limit reached, stop buffering
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// chunk is already a copy (safe to retain)
	b.chunks = append(b.chunks, chunk)
	b.totalBytes.Add(uint64(len(chunk)))

	return nil
}

// GetTotalBytes returns total bytes buffered
func (b *DecompressedStreamBuffer) GetTotalBytes() uint64 {
	return b.totalBytes.Load()
}

// GetChunkCount returns number of chunks buffered
func (b *DecompressedStreamBuffer) GetChunkCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.chunks)
}

// SaveToFile writes buffered decompressed tar to disk
func (b *DecompressedStreamBuffer) SaveToFile(path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriterSize(f, 1024*1024) // 1MB buffer
	for _, chunk := range b.chunks {
		if _, err := writer.Write(chunk); err != nil {
			return err
		}
	}

	return writer.Flush()
}

// FileExtractor extracts tar contents to a directory
type FileExtractor struct {
	outputDir       string
	currentFile     *os.File
	bufferedWrite   *bufio.Writer // buffered writes to prevent pipeline blocking
	totalBytes      atomic.Uint64 // total bytes written (for progress)
	filesExtracted  atomic.Uint64 // total files extracted
	lastProgressLog time.Time
}

// OnFileStart creates directory or file for extraction
func (f *FileExtractor) OnFileStart(meta *tarstream.FileMetadata) (tarstream.CallbackAction, error) {
	fmt.Printf("Extracting: %s (%d bytes)\n", meta.Path, meta.Size)

	if meta.IsDirectory {
		path := filepath.Join(f.outputDir, meta.Path)
		return tarstream.ActionContinue, os.MkdirAll(path, 0755)
	}

	path := filepath.Join(f.outputDir, meta.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return tarstream.ActionStop, err
	}

	file, err := os.Create(path)
	if err != nil {
		return tarstream.ActionStop, err
	}
	f.currentFile = file
	// use 256KB buffered writer to prevent blocking pipeline on slow disk writes
	f.bufferedWrite = bufio.NewWriterSize(file, 256*1024)

	return tarstream.ActionContinue, nil
}

// OnFileChunk writes chunk to current file (buffered)
func (f *FileExtractor) OnFileChunk(chunk []byte) (tarstream.CallbackAction, error) {
	if f.bufferedWrite != nil {
		n, err := f.bufferedWrite.Write(chunk)
		if err != nil {
			return tarstream.ActionStop, err
		}

		// update progress counters
		f.totalBytes.Add(uint64(n))

		// periodic progress reporting (every 5 seconds)
		now := time.Now()
		if now.Sub(f.lastProgressLog) > 5*time.Second {
			totalMB := float64(f.totalBytes.Load()) / 1024 / 1024
			files := f.filesExtracted.Load()
			fmt.Fprintf(os.Stderr, "\r[Progress] Files: %d, Data: %.2f MB", files, totalMB)
			f.lastProgressLog = now
		}

		// flush buffer every 4MB to prevent huge memory buildup
		if f.bufferedWrite.Buffered() > 4*1024*1024 {
			if err := f.bufferedWrite.Flush(); err != nil {
				return tarstream.ActionStop, err
			}
		}

		return tarstream.ActionContinue, nil
	}
	return tarstream.ActionContinue, nil
}

// OnFileEnd closes current file
func (f *FileExtractor) OnFileEnd(meta *tarstream.FileMetadata) (tarstream.CallbackAction, error) {
	if f.bufferedWrite != nil {
		// flush buffered data before closing
		if err := f.bufferedWrite.Flush(); err != nil {
			return tarstream.ActionStop, err
		}
		f.bufferedWrite = nil
	}
	if f.currentFile != nil {
		f.currentFile.Close()
		f.currentFile = nil
	}

	// track completed files
	f.filesExtracted.Add(1)

	return tarstream.ActionContinue, nil
}

// OnError handles errors during extraction
func (f *FileExtractor) OnError(err error) tarstream.CallbackAction {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	return tarstream.ActionStop
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <url> <output_dir> [--buffer-decompressed] [--max-buffer-mb <size>]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.gz ./output\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.gz ./output --buffer-decompressed --max-buffer-mb 100\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		fmt.Fprintf(os.Stderr, "  --buffer-decompressed      Store decompressed tar in memory (for re-processing)\n")
		fmt.Fprintf(os.Stderr, "  --max-buffer-mb <size>     Max MB to buffer (0 = unlimited, default: 0)\n")
		os.Exit(1)
	}

	url := os.Args[1]
	outputDir := os.Args[2]

	// parse optional flags
	bufferEnabled := false
	maxBufferMB := uint64(0)
	for i := 3; i < len(os.Args); i++ {
		if os.Args[i] == "--buffer-decompressed" {
			bufferEnabled = true
		} else if os.Args[i] == "--max-buffer-mb" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &maxBufferMB)
			i++
		}
	}

	// create HTTP client with optimized settings for large downloads
	httpClient := &http.Client{
		Timeout: 0, // no overall timeout for streaming
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// increase buffer sizes for high throughput
			ReadBufferSize:  256 * 1024, // 256KB read buffer
			WriteBufferSize: 256 * 1024, // 256KB write buffer
		},
	}

	// create optional in-memory buffer for decompressed stream
	var streamBuffer *DecompressedStreamBuffer
	if bufferEnabled {
		maxBufferBytes := maxBufferMB * 1024 * 1024
		streamBuffer = NewDecompressedStreamBuffer(maxBufferBytes, true)
		fmt.Printf("Decompressed stream buffering: ENABLED (max: %d MB)\n", maxBufferMB)
	}

	// create extractor with optimized settings for large files
	extractor := tarstream.New().
		WithCompression(tarstream.Auto).        // auto-detect (uses adaptive buffer sizing)
		WithChannelCapacity(64).                // large capacity for 100GB files (64 * 256KB = 16MB pipeline depth)
		WithHTTPClient(httpClient)              // use optimized HTTP client

	// add stream callback if buffering enabled
	if streamBuffer != nil {
		extractor = extractor.WithStreamCallback(streamBuffer)
	}

	callback := &FileExtractor{
		outputDir:       outputDir,
		lastProgressLog: time.Now(),
	}

	fmt.Printf("Downloading and extracting: %s\n", url)
	fmt.Printf("Output directory: %s\n", outputDir)
	if streamBuffer != nil {
		fmt.Printf("Pipeline: HTTP Download → Decompress → [Buffer in Memory] + [Extract Files] (all parallel)\n\n")
	} else {
		fmt.Printf("Pipeline: HTTP Download → Decompress → Extract (all parallel)\n\n")
	}

	ctx := context.Background()
	stats, err := extractor.ExtractFromURL(ctx, url, callback)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Extraction failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n\n✓ Extraction complete!\n")
	fmt.Printf("  Files: %d\n", stats.TotalFiles)
	fmt.Printf("  Bytes: %d (%.2f GB)\n", stats.TotalBytes, float64(stats.TotalBytes)/1024/1024/1024)
	fmt.Printf("  Duration: %v\n", stats.Duration)
	fmt.Printf("  Throughput: %.2f MB/s\n", float64(stats.TotalBytes)/1024/1024/stats.Duration.Seconds())

	// show stream buffer stats if enabled
	if streamBuffer != nil {
		bufferedMB := float64(streamBuffer.GetTotalBytes()) / 1024 / 1024
		fmt.Printf("\n✓ Decompressed stream buffering:\n")
		fmt.Printf("  Chunks: %d\n", streamBuffer.GetChunkCount())
		fmt.Printf("  Bytes buffered: %d (%.2f MB)\n", streamBuffer.GetTotalBytes(), bufferedMB)

		// optionally save to disk
		tarPath := filepath.Join(outputDir, "decompressed.tar")
		fmt.Printf("\nSaving decompressed tar to: %s\n", tarPath)
		if err := streamBuffer.SaveToFile(tarPath); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to save decompressed tar: %v\n", err)
		} else {
			fmt.Printf("✓ Decompressed tar saved successfully\n")
			fmt.Printf("\nYou can now:\n")
			fmt.Printf("  • Re-extract without downloading: tar -xf %s\n", tarPath)
			fmt.Printf("  • Compress with different format: zstd %s\n", tarPath)
			fmt.Printf("  • Verify integrity: sha256sum %s\n", tarPath)
		}
	}

	fmt.Printf("\nPipeline worked in parallel:\n")
	fmt.Printf("  • HTTP download (goroutine 1)\n")
	fmt.Printf("  • Decompression (goroutine 2)\n")
	if streamBuffer != nil {
		fmt.Printf("  • Stream buffering (goroutine 3) - async, non-blocking\n")
		fmt.Printf("  • Tar extraction (main goroutine)\n")
	} else {
		fmt.Printf("  • Tar extraction (main goroutine)\n")
	}
}
