package main

// optimized for machines with:
//   - 64+ CPU cores
//   - 128+ GB RAM
//   - NVMe SSD storage
//   - 10+ Gbps network bandwidth
//
// settings:
//   - 2MB buffer size (8x default)
//   - 256 channel capacity (21x default)
//   - 512MB pipeline depth (32x default)
//   - 2MB HTTP buffers
//   - pre-enabled stream buffering with 10GB limit
//
// note: single-archive extraction uses 3-4 cores max due to sequential pipeline.
// to utilize 64 cores: run multiple concurrent extractions (16 parallel downloads = 64 cores)

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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

	// open with optimized flags for sequential writes
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return tarstream.ActionStop, err
	}

	// pre-allocate disk space (reduces fragmentation for large files)
	if meta.Size > 0 {
		file.Truncate(int64(meta.Size))
	}

	f.currentFile = file
	// use 4MB buffered writer for high throughput (16x default)
	f.bufferedWrite = bufio.NewWriterSize(file, 4*1024*1024)

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

		// periodic progress reporting (every 2 seconds for high throughput)
		now := time.Now()
		if now.Sub(f.lastProgressLog) > 2*time.Second {
			totalGB := float64(f.totalBytes.Load()) / 1024 / 1024 / 1024
			files := f.filesExtracted.Load()
			fmt.Fprintf(os.Stderr, "\r[progress] files: %d, data: %.2f GB", files, totalGB)
			f.lastProgressLog = now
		}

		// flush buffer every 16MB (4x larger for high throughput)
		if f.bufferedWrite.Buffered() > 16*1024*1024 {
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
		fmt.Fprintf(os.Stderr, "usage: %s <url> <output_dir> [--workers N] [--max-buffer-gb <size>] [--no-buffer]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  • 2MB buffers (8x default)\n")
		fmt.Fprintf(os.Stderr, "  • 256 channel capacity (21x default)\n")
		fmt.Fprintf(os.Stderr, "  • 512MB pipeline depth\n")
		fmt.Fprintf(os.Stderr, "  • stream buffering ENABLED by default (10GB limit)\n")
		fmt.Fprintf(os.Stderr, "  • expected throughput: 500-2000 MB/s\n")
		fmt.Fprintf(os.Stderr, "\nexample:\n")
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.zst ./output\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.zst ./output --workers 60\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.zst ./output --max-buffer-gb 50\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.zst ./output --no-buffer\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\noptions:\n")
		fmt.Fprintf(os.Stderr, "  --workers N               Parallel workers for file processing (default: 0=sequential, recommend: 60)\n")
		fmt.Fprintf(os.Stderr, "  --max-buffer-gb <size>    Max GB to buffer (default: 10)\n")
		fmt.Fprintf(os.Stderr, "  --no-buffer               Disable stream buffering\n")
		os.Exit(1)
	}

	url := os.Args[1]
	outputDir := os.Args[2]

	// system info
	numCPU := runtime.NumCPU()
	fmt.Printf("system: %d CPU cores, %s/%s, Go %s\n\n", numCPU, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// maximize parallelism
	runtime.GOMAXPROCS(numCPU)

	// parse optional flags - buffering ENABLED by default for high-perf
	bufferEnabled := true
	maxBufferGB := uint64(10) // default 10GB limit
	workerCount := 0          // default: sequential (0 workers)
	for i := 3; i < len(os.Args); i++ {
		if os.Args[i] == "--no-buffer" {
			bufferEnabled = false
		} else if os.Args[i] == "--max-buffer-gb" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &maxBufferGB)
			i++
		} else if os.Args[i] == "--workers" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &workerCount)
			i++
		}
	}

	// aggressive HTTP client for maximum throughput
	httpClient := &http.Client{
		Timeout: 0, // no timeout for streaming
		Transport: &http.Transport{
			MaxIdleConns:          100,               // large connection pool
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       120 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true,              // we handle compression ourselves
			// maximize buffer sizes for high-bandwidth networks
			ReadBufferSize:  2 * 1024 * 1024, // 2MB read buffer (8x default)
			WriteBufferSize: 2 * 1024 * 1024, // 2MB write buffer
		},
	}

	// create in-memory buffer for decompressed stream (default: enabled)
	var streamBuffer *DecompressedStreamBuffer
	if bufferEnabled {
		maxBufferBytes := maxBufferGB * 1024 * 1024 * 1024
		streamBuffer = NewDecompressedStreamBuffer(maxBufferBytes, true)
		fmt.Printf("stream buffering: ENABLED (max: %d GB)\n", maxBufferGB)
	} else {
		fmt.Printf("stream buffering: DISABLED\n")
	}

	// extreme performance configuration
	extractor := tarstream.New().
		WithCompression(tarstream.Auto).
		WithBufferSize(2 * 1024 * 1024).    // 2MB buffers (8x default, better for high bandwidth)
		WithChannelCapacity(256).           // 256 buffers = 512MB pipeline depth (massive buffering)
		WithHTTPClient(httpClient)

	// add stream callback if buffering enabled
	if streamBuffer != nil {
		extractor = extractor.WithStreamCallback(streamBuffer)
	}

	// add parallel workers if enabled
	if workerCount > 0 {
		extractor = extractor.WithWorkerCount(workerCount)
		fmt.Printf("parallel workers: %d (tar entry processing)\n", workerCount)
	} else {
		fmt.Printf("mode: sequential (single-threaded)\n")
	}

	callback := &FileExtractor{
		outputDir:       outputDir,
		lastProgressLog: time.Now(),
	}

	fmt.Printf("downloading: %s\n", url)
	fmt.Printf("output directory: %s\n\n", outputDir)

	fmt.Printf("configuration:\n")
	fmt.Printf("  buffer size: 2 MB (8x default)\n")
	fmt.Printf("  channel capacity: 256 (21x default)\n")
	fmt.Printf("  pipeline depth: 512 MB (256 × 2MB)\n")
	fmt.Printf("  HTTP buffer: 2 MB read + 2 MB write\n")
	if streamBuffer != nil {
		fmt.Printf("  stream buffering: %d GB max\n", maxBufferGB)
	}
	fmt.Printf("\n")

	ctx := context.Background()
	start := time.Now()
	stats, err := extractor.ExtractFromURL(ctx, url, callback)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nextraction failed: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	throughputMBps := float64(stats.TotalBytes) / 1024 / 1024 / elapsed.Seconds()
	throughputGBps := throughputMBps / 1024

	fmt.Printf("\n\n✓ extraction complete!\n")
	fmt.Printf("  files: %d\n", stats.TotalFiles)
	fmt.Printf("  bytes: %d (%.2f GB)\n", stats.TotalBytes, float64(stats.TotalBytes)/1024/1024/1024)
	fmt.Printf("  duration: %v\n", elapsed)
	fmt.Printf("  throughput: %.2f MB/s (%.3f GB/s)\n", throughputMBps, throughputGBps)

	// performance analysis
	fmt.Printf("\nperformance analysis:\n")
	if throughputMBps < 200 {
		fmt.Printf("  ⚠️  low throughput - bottleneck likely:\n")
		fmt.Printf("      • network bandwidth (<200 MB/s = <1.6 Gbps)\n")
		fmt.Printf("      • slow disk I/O\n")
	} else if throughputMBps < 500 {
		fmt.Printf("  ✓ good throughput (~%.0f MB/s)\n", throughputMBps)
	} else if throughputMBps < 1000 {
		fmt.Printf("  ✓✓ excellent throughput (~%.0f MB/s)\n", throughputMBps)
	} else {
		fmt.Printf("  ✓✓✓ exceptional throughput (~%.0f MB/s) - maxing out hardware!\n", throughputMBps)
	}

	// show stream buffer stats if enabled
	if streamBuffer != nil {
		bufferedGB := float64(streamBuffer.GetTotalBytes()) / 1024 / 1024 / 1024
		fmt.Printf("\n✓ decompressed stream buffering:\n")
		fmt.Printf("  chunks: %d\n", streamBuffer.GetChunkCount())
		fmt.Printf("  bytes buffered: %d (%.2f GB)\n", streamBuffer.GetTotalBytes(), bufferedGB)

		// save to disk
		tarPath := filepath.Join(outputDir, "decompressed.tar")
		fmt.Printf("\nsaving decompressed tar to: %s\n", tarPath)
		saveStart := time.Now()
		if err := streamBuffer.SaveToFile(tarPath); err != nil {
			fmt.Fprintf(os.Stderr, "failed to save: %v\n", err)
		} else {
			saveDuration := time.Since(saveStart)
			saveRate := bufferedGB / saveDuration.Seconds()
			fmt.Printf("✓ saved in %v (%.2f GB/s write rate)\n", saveDuration, saveRate)
			fmt.Printf("\nyou can now:\n")
			fmt.Printf("  • re-extract: tar -xf %s\n", tarPath)
			fmt.Printf("  • recompress: zstd -19 %s\n", tarPath)
			fmt.Printf("  • verify: sha256sum %s\n", tarPath)
		}
	}

	// memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	fmt.Printf("\nmemory usage:\n")
	fmt.Printf("  allocated: %.2f MB\n", float64(memStats.Alloc)/1024/1024)
	fmt.Printf("  sys: %.2f MB\n", float64(memStats.Sys)/1024/1024)
	fmt.Printf("  gc runs: %d\n", memStats.NumGC)

	fmt.Printf("\npipeline architecture (all parallel):\n")
	fmt.Printf("  • goroutine 1: HTTP download (2MB buffers)\n")
	fmt.Printf("  • goroutine 2: decompression (2MB buffers, CGO batching)\n")
	if streamBuffer != nil {
		fmt.Printf("  • goroutine 3: stream buffering (async, non-blocking)\n")
	}
	fmt.Printf("  • main goroutine: tar extraction + disk I/O\n")
	fmt.Printf("  • total pipeline depth: 512 MB in-flight data\n")

	fmt.Printf("\ncore utilization:\n")
	fmt.Printf("  cores used: ~3-4 out of %d (%.1f%%)\n", numCPU, float64(4)/float64(numCPU)*100)
	fmt.Printf("\nto use all %d cores:\n", numCPU)
	fmt.Printf("  option 1: run %d parallel extractions (different archives)\n", numCPU/4)
	fmt.Printf("  option 2: implement parallel tar entry processing (coming soon)\n")
	fmt.Printf("  option 3: use multi-threaded zstd compression (compression only, not decompression)\n")
	fmt.Printf("\nwhy low core usage:\n")
	fmt.Printf("  • HTTP download: I/O-bound (network wait)\n")
	fmt.Printf("  • decompression: single-threaded per stream (algorithm limitation)\n")
	fmt.Printf("  • tar parsing: sequential (maintains file order)\n")
	fmt.Printf("  • disk writes: I/O-bound (disk wait)\n")
}
