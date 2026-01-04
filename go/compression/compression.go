package compression

import "strings"

// CompressionType specifies the compression format
type CompressionType uint8

const (
	// None indicates uncompressed tar
	None CompressionType = iota
	// Gzip indicates gzip compression (.tar.gz, .tgz)
	Gzip
	// Bzip2 indicates bzip2 compression (.tar.bz2, .tbz2)
	Bzip2
	// Xz indicates xz compression (.tar.xz, .txz)
	Xz
	// Zstd indicates zstd compression (.tar.zst, .tzst)
	Zstd
	// Auto enables auto-detection from URL extension
	Auto
)

// DetectFromURL infers compression type from URL extension
func DetectFromURL(url string) CompressionType {
	lower := strings.ToLower(url)

	if strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".tgz") {
		return Gzip
	} else if strings.HasSuffix(lower, ".tar.bz2") || strings.HasSuffix(lower, ".tbz2") {
		return Bzip2
	} else if strings.HasSuffix(lower, ".tar.xz") || strings.HasSuffix(lower, ".txz") {
		return Xz
	} else if strings.HasSuffix(lower, ".tar.zst") || strings.HasSuffix(lower, ".tzst") {
		return Zstd
	} else if strings.HasSuffix(lower, ".tar") {
		return None
	}

	return Auto // fallback: implement magic number detection
}

// OptimalBufferSize returns the optimal buffer size for each compression type.
// larger buffers improve throughput for compression algorithms with large windows/dictionaries.
//
// buffer sizes are tuned based on compression algorithm characteristics:
//   - gzip: 64KB (optimal for 32KB window size)
//   - zstd: 128KB (optimal for 128KB window size)
//   - bzip2: 256KB (optimal for 900KB window, balances memory vs throughput)
//   - xz: 512KB (optimal for 8MB dictionary, significant speedup)
//   - none: 64KB (standard tar block alignment)
func (c CompressionType) OptimalBufferSize() int {
	switch c {
	case Gzip:
		return 64 * 1024 // 64KB
	case Zstd:
		return 128 * 1024 // 128KB
	case Bzip2:
		return 256 * 1024 // 256KB
	case Xz:
		return 512 * 1024 // 512KB
	case None:
		return 64 * 1024 // 64KB
	default:
		return 64 * 1024 // conservative default
	}
}
