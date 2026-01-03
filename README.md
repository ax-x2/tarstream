# tarstream

rust library for streaming large tar files during download and extracting them on-the-fly with zero-copy processing.

## features

- stream 100gb+ tar files without loading into memory
- process files during download (no waiting for full download)
- support for tar, tar.gz, tar.bz2, tar.xz
- zero-copy with callback-based architecture
- filter and extract only matching files

## installation

```toml
[dependencies]
tarstream = { path = "." }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## quick start

### basic extraction

```rust
use tarstream::*;
use std::fs::File;
use std::io::Write;

struct Extractor {
    current_file: Option<File>,
}

impl FileCallback for Extractor {
    fn on_file_start(&mut self, metadata: &FileMetadata) -> Result<CallbackAction> {
        if !metadata.is_directory {
            self.current_file = Some(File::create(&metadata.path)?);
        }
        Ok(CallbackAction::Continue)
    }

    fn on_file_chunk(&mut self, chunk: &[u8]) -> Result<CallbackAction> {
        if let Some(file) = &mut self.current_file {
            file.write_all(chunk)?;
        }
        Ok(CallbackAction::Continue)
    }

    fn on_file_end(&mut self, _: &FileMetadata) -> Result<CallbackAction> {
        self.current_file = None;
        Ok(CallbackAction::Continue)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let extractor = TarStreamExtractor::new()
        .with_compression(CompressionType::Auto);

    let mut callback = Extractor { current_file: None };

    extractor.extract_from_url(
        "https://example.com/archive.tar.gz",
        &mut callback
    ).await?;

    Ok(())
}
```

### selective extraction with filtering

```rust
use tarstream::*;

struct Filter {
    skip_current: bool,
}

impl FileCallback for Filter {
    fn on_file_start(&mut self, metadata: &FileMetadata) -> Result<CallbackAction> {
        if metadata.path.ends_with(".jpg") {
            println!("found: {}", metadata.path);
            self.skip_current = false;
            Ok(CallbackAction::Continue)
        } else {
            self.skip_current = true;
            Ok(CallbackAction::Skip)
        }
    }

    fn on_file_chunk(&mut self, chunk: &[u8]) -> Result<CallbackAction> {
        if !self.skip_current {
            // process chunk
        }
        Ok(CallbackAction::Continue)
    }

    fn on_file_end(&mut self, _: &FileMetadata) -> Result<CallbackAction> {
        Ok(CallbackAction::Continue)
    }
}
```

## examples

### extract all files

```bash
cargo run --example basic_download https://example.com/archive.tar.gz ./output
```

### extract only matching files

```bash
# extract only jpg images
cargo run --example custom_filter https://example.com/data.tar.gz '*.jpg' ./images

# extract json files from data directory
cargo run --example custom_filter https://example.com/logs.tar.gz 'data/*.json' ./output

# extract all csv files anywhere
cargo run --example custom_filter https://example.com/archive.tar.gz '**/*.csv' ./csv
```

## how it works

1. streams http download using reqwest
2. decompresses on-the-fly (gzip/bzip2/xz)
3. parses tar entries as they arrive
4. calls your callback with file chunks (zero-copy)
5. non-matching files are discarded from memory immediately

## performance

- **zero-copy**: passes `&[u8]` slices to callbacks
- **zero-allocation**: reuses buffers between files
- **streaming**: processes 100gb files with constant memory usage
- **efficient**: skipped files never touch disk or stay in memory

## api

### tarstream extractor

```rust
let extractor = TarStreamExtractor::new()
    .with_compression(CompressionType::Auto)  // auto-detect or specify
    .with_buffer_size(64 * 1024)              // 64kb chunks
    .with_max_file_size(1024 * 1024 * 1024);  // 1gb limit

let stats = extractor.extract_from_url(url, &mut callback).await?;
```

### callback actions

- `CallbackAction::Continue` - process this chunk and continue
- `CallbackAction::Skip` - skip rest of current file
- `CallbackAction::Stop` - stop entire extraction

### compression types

- `CompressionType::None` - plain tar
- `CompressionType::Gzip` - .tar.gz
- `CompressionType::Bzip2` - .tar.bz2
- `CompressionType::Xz` - .tar.xz
- `CompressionType::Auto` - detect from url extension

## license

mit
