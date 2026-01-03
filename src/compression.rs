use crate::error::Result;
use crate::CompressionType;
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use std::io::Read;
use tokio::io::{AsyncRead, AsyncReadExt};
use xz2::read::XzDecoder;

pub(crate) struct SyncBridge<R: AsyncRead + Unpin> {
    inner: R,
    runtime_handle: tokio::runtime::Handle,
}

impl<R: AsyncRead + Unpin> SyncBridge<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            runtime_handle: tokio::runtime::Handle::current(),
        }
    }
}

impl<R: AsyncRead + Unpin> Read for SyncBridge<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        tokio::task::block_in_place(|| self.runtime_handle.block_on(self.inner.read(buf)))
    }
}

pub(crate) enum Decompressor<R: AsyncRead + Unpin> {
    None(R),
    Gzip(GzDecoder<SyncBridge<R>>),
    Bzip2(BzDecoder<SyncBridge<R>>),
    Xz(XzDecoder<SyncBridge<R>>),
}

impl<R: AsyncRead + Unpin> Decompressor<R> {
    pub fn new(reader: R, compression: CompressionType) -> Self {
        match compression {
            CompressionType::None => Decompressor::None(reader),
            CompressionType::Gzip => {
                let bridge = SyncBridge::new(reader);
                Decompressor::Gzip(GzDecoder::new(bridge))
            }
            CompressionType::Bzip2 => {
                let bridge = SyncBridge::new(reader);
                Decompressor::Bzip2(BzDecoder::new(bridge))
            }
            CompressionType::Xz => {
                let bridge = SyncBridge::new(reader);
                Decompressor::Xz(XzDecoder::new(bridge))
            }
            CompressionType::Auto => Decompressor::None(reader),
        }
    }
}

impl<R: AsyncRead + Unpin> Read for Decompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Decompressor::None(reader) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(reader.read(buf))
            }),
            Decompressor::Gzip(decoder) => decoder.read(buf),
            Decompressor::Bzip2(decoder) => decoder.read(buf),
            Decompressor::Xz(decoder) => decoder.read(buf),
        }
    }
}

pub(crate) async fn detect_compression<R>(reader: &mut R) -> Result<CompressionType>
where
    R: AsyncRead + Unpin,
{
    let mut magic = [0u8; 6];
    let n = reader.read(&mut magic).await?;

    if n < 2 {
        return Ok(CompressionType::None);
    }

    match &magic[..2] {
        [0x1f, 0x8b] => Ok(CompressionType::Gzip),
        [0x42, 0x5a] => Ok(CompressionType::Bzip2),
        _ if n >= 6 && &magic[..6] == b"\xfd7zXZ\x00" => Ok(CompressionType::Xz),
        _ => Ok(CompressionType::None),
    }
}
