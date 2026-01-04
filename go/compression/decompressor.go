package compression

import (
	"fmt"
	"io"
)

// decompressor is the interface for all CGO decompressors.
//
// - Write() stages compressed input (avoids per-write CGO overhead)
// - Read() processes staged input and returns decompressed data
// - ReadBatch() processes multiple outputs in single CGO call (70-90% less overhead)
// - Each instance is goroutine-local (not thread-safe)
type Decompressor interface {
	// write stages compressed data for decompression
	Write(compressed []byte) error

	// read returns decompressed data from staged input
	Read(output []byte) (int, error)

	// readbatch fills multiple output buffers in a single CGO transition.
	// reduces CGO boundary crossing overhead by processing 4-8 buffers at once.
	// returns slice of bytes written to each buffer (may be shorter than len(outputs)).
	// returns io.EOF when no more data available from staged input.
	ReadBatch(outputs [][]byte) ([]int, error)

	// close releases decompressor resources
	Close() error
}

// NewDecompressor creates a decompressor based on compression type
func NewDecompressor(t CompressionType) (Decompressor, error) {
	switch t {
	case None:
		return NewPassthroughDecompressor(), nil
	case Gzip:
		return NewCGOGzipDecompressor()
	case Bzip2:
		return NewCGOBzip2Decompressor()
	case Xz:
		return NewCGOXzDecompressor()
	case Zstd:
		return NewCGOZstdDecompressor()
	default:
		return nil, fmt.Errorf("invalid compression type: %d", t)
	}
}

// PassthroughDecompressor handles uncompressed data (no-op)
type PassthroughDecompressor struct {
	data []byte
}

// NewPassthroughDecompressor creates a passthrough decompressor for uncompressed data
func NewPassthroughDecompressor() *PassthroughDecompressor {
	return &PassthroughDecompressor{}
}

func (d *PassthroughDecompressor) Write(data []byte) error {
	d.data = data
	return nil
}

func (d *PassthroughDecompressor) Read(output []byte) (int, error) {
	if len(d.data) == 0 {
		return 0, nil
	}

	n := copy(output, d.data)
	d.data = d.data[n:]

	if len(d.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (d *PassthroughDecompressor) ReadBatch(outputs [][]byte) ([]int, error) {
	sizes := make([]int, 0, len(outputs))

	for _, output := range outputs {
		if len(d.data) == 0 {
			break
		}

		n := copy(output, d.data)
		d.data = d.data[n:]
		sizes = append(sizes, n)

		if len(d.data) == 0 {
			return sizes, io.EOF
		}
	}

	if len(sizes) == 0 {
		return sizes, io.EOF
	}
	return sizes, nil
}

func (d *PassthroughDecompressor) Close() error {
	return nil
}
