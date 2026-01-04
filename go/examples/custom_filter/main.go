package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yourorg/tarstream"
)

// FilteredExtractor extracts only files matching a pattern
type FilteredExtractor struct {
	outputDir   string
	pattern     string // simple substring match
	currentFile *os.File
	matched     int
	skipped     int
}

// OnFileStart filters files by pattern
func (f *FilteredExtractor) OnFileStart(meta *tarstream.FileMetadata) (tarstream.CallbackAction, error) {
	// check if file matches pattern
	if !strings.Contains(meta.Path, f.pattern) {
		f.skipped++
		// avoids downloading file data
		return tarstream.ActionSkip, nil
	}

	f.matched++
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

	return tarstream.ActionContinue, nil
}

// OnFileChunk writes chunk to current file
func (f *FilteredExtractor) OnFileChunk(chunk []byte) (tarstream.CallbackAction, error) {
	if f.currentFile != nil {
		_, err := f.currentFile.Write(chunk)
		return tarstream.ActionContinue, err
	}
	return tarstream.ActionContinue, nil
}

// OnFileEnd closes current file
func (f *FilteredExtractor) OnFileEnd(meta *tarstream.FileMetadata) (tarstream.CallbackAction, error) {
	if f.currentFile != nil {
		f.currentFile.Close()
		f.currentFile = nil
	}
	return tarstream.ActionContinue, nil
}

// OnError handles errors during extraction
func (f *FilteredExtractor) OnError(err error) tarstream.CallbackAction {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	return tarstream.ActionStop
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <url> <pattern> <output_dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s https://example.com/archive.tar.gz '.txt' ./output\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nThis extracts only files containing '.txt' in their path.\n")
		os.Exit(1)
	}

	url := os.Args[1]
	pattern := os.Args[2]
	outputDir := os.Args[3]

	// create extractor with auto compression detection
	extractor := tarstream.New().
		WithCompression(tarstream.Auto).
		WithBufferSize(64 * 1024)

	callback := &FilteredExtractor{
		outputDir: outputDir,
		pattern:   pattern,
	}

	fmt.Printf("Downloading and filtering: %s\n", url)
	fmt.Printf("Pattern: %s\n", pattern)
	fmt.Printf("Output directory: %s\n\n", outputDir)

	ctx := context.Background()
	stats, err := extractor.ExtractFromURL(ctx, url, callback)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Extraction failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nExtraction complete!\n")
	fmt.Printf("  Total files in archive: %d\n", stats.TotalFiles)
	fmt.Printf("  Matched files: %d\n", callback.matched)
	fmt.Printf("  Skipped files: %d\n", callback.skipped)
	fmt.Printf("  Bytes extracted: %d\n", stats.TotalBytes)
	fmt.Printf("  Duration: %v\n", stats.Duration)
	fmt.Printf("  Throughput: %.2f MB/s\n", float64(stats.TotalBytes)/1024/1024/stats.Duration.Seconds())
	fmt.Printf("\nSkip optimization saved bandwidth by not downloading %d files!\n", callback.skipped)
}
