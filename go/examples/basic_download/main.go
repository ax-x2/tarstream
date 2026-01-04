package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	tarstream "github.com/ax-x2/tarstream/go"
)

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
		fmt.Fprintf(os.Stderr, "Usage: %s <url> <output_dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.gz ./output\n", os.Args[0])
		os.Exit(1)
	}

	url := os.Args[1]
	outputDir := os.Args[2]

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

	// create extractor with optimized settings for large files
	extractor := tarstream.New().
		WithCompression(tarstream.Auto).        // auto-detect (uses adaptive buffer sizing)
		WithChannelCapacity(64).                // large capacity for 100GB files (64 * 256KB = 16MB pipeline depth)
		WithHTTPClient(httpClient)              // use optimized HTTP client

	callback := &FileExtractor{
		outputDir:       outputDir,
		lastProgressLog: time.Now(),
	}

	fmt.Printf("Downloading and extracting: %s\n", url)
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Printf("Pipeline: HTTP Download → Decompress → Extract (all parallel)\n\n")

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
	fmt.Printf("\nPipeline worked in parallel:\n")
	fmt.Printf("  • HTTP download (goroutine 1)\n")
	fmt.Printf("  • Decompression (goroutine 2)\n")
	fmt.Printf("  • Tar extraction (main goroutine)\n")
}
