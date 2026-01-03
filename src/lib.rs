mod callback;
mod compression;
mod error;
mod stream;

pub use callback::{CallbackAction, FileCallback, FileMetadata};
pub use error::{Error, Result};

use futures_util::TryStreamExt;
use std::time::{Duration, Instant};
use tokio_util::io::StreamReader;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Bzip2,
    Xz,
    Auto,
}

#[derive(Debug, Clone)]
pub struct ExtractionStats {
    pub total_files: u64,
    pub total_bytes: u64,
    pub duration: Duration,
}

pub struct TarStreamExtractor {
    compression: CompressionType,
    buffer_size: usize,
    max_file_size: Option<u64>,
}

impl Default for TarStreamExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl TarStreamExtractor {
    pub fn new() -> Self {
        Self {
            compression: CompressionType::Auto,
            buffer_size: 64 * 1024,
            max_file_size: None,
        }
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn with_max_file_size(mut self, max: u64) -> Self {
        self.max_file_size = Some(max);
        self
    }

    pub async fn extract_from_url<F>(
        &self,
        url: &str,
        callback: &mut F,
    ) -> Result<ExtractionStats>
    where
        F: FileCallback,
    {
        let start = Instant::now();

        let response = reqwest::get(url).await?;

        let compression = if matches!(self.compression, CompressionType::Auto) {
            detect_compression_from_url(url)
        } else {
            self.compression
        };

        let stream = response.bytes_stream().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, e)
        });
        let reader = StreamReader::new(stream);

        let mut stats = stream::extract_tar_stream(
            reader,
            compression,
            callback,
            self.buffer_size,
            self.max_file_size,
        )
        .await?;

        stats.duration = start.elapsed();
        Ok(stats)
    }

    pub async fn extract_from_reader<R, F>(
        &self,
        reader: R,
        callback: &mut F,
    ) -> Result<ExtractionStats>
    where
        R: tokio::io::AsyncRead + Unpin,
        F: FileCallback,
    {
        let start = Instant::now();

        let mut stats = stream::extract_tar_stream(
            reader,
            self.compression,
            callback,
            self.buffer_size,
            self.max_file_size,
        )
        .await?;

        stats.duration = start.elapsed();
        Ok(stats)
    }
}

fn detect_compression_from_url(url: &str) -> CompressionType {
    if url.ends_with(".tar.gz") || url.ends_with(".tgz") {
        CompressionType::Gzip
    } else if url.ends_with(".tar.bz2") || url.ends_with(".tbz2") {
        CompressionType::Bzip2
    } else if url.ends_with(".tar.xz") || url.ends_with(".txz") {
        CompressionType::Xz
    } else if url.ends_with(".tar") {
        CompressionType::None
    } else {
        CompressionType::Auto
    }
}
