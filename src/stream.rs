use crate::callback::{CallbackAction, FileCallback, FileMetadata};
use crate::compression::Decompressor;
use crate::error::{Error, Result};
use crate::{CompressionType, ExtractionStats};
use std::io::Read;
use std::time::{Duration, SystemTime};
use tar::Archive;
use tokio::io::AsyncRead;

pub(crate) async fn extract_tar_stream<R, F>(
    reader: R,
    compression: CompressionType,
    callback: &mut F,
    buffer_size: usize,
    max_file_size: Option<u64>,
) -> Result<ExtractionStats>
where
    R: AsyncRead + Unpin,
    F: FileCallback + ?Sized,
{
    let decompressor = Decompressor::new(reader, compression);

    let mut archive = Archive::new(decompressor);

    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;

    let entries = archive
        .entries()
        .map_err(|e| Error::TarError(e.to_string()))?;

    for entry_result in entries {
        let mut entry = entry_result.map_err(|e| Error::TarError(e.to_string()))?;

        let header = entry.header();
        let path = entry
            .path()
            .map_err(|e| Error::TarError(e.to_string()))?
            .to_string_lossy()
            .into_owned();

        let size = header.size().map_err(|e| Error::TarError(e.to_string()))?;
        let mode = header.mode().map_err(|e| Error::TarError(e.to_string()))?;
        let mtime = header.mtime().ok().and_then(|secs| {
            SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_secs(secs))
        });

        let is_directory = header
            .entry_type()
            .is_dir();

        let metadata = FileMetadata {
            path: path.clone(),
            size,
            mode,
            modified_time: mtime,
            is_directory,
        };

        if let Some(max_size) = max_file_size {
            if size > max_size {
                return Err(Error::FileTooLarge {
                    size,
                    max: max_size,
                });
            }
        }

        match callback.on_file_start(&metadata)? {
            CallbackAction::Skip => {
                total_files += 1;
                continue;
            }
            CallbackAction::Stop => break,
            CallbackAction::Continue => {}
        }

        if !is_directory {
            let mut buffer = vec![0u8; buffer_size];
            loop {
                let n = entry
                    .read(&mut buffer)
                    .map_err(|e| Error::IoError(e))?;
                if n == 0 {
                    break;
                }

                total_bytes += n as u64;

                match callback.on_file_chunk(&buffer[..n])? {
                    CallbackAction::Skip => break,
                    CallbackAction::Stop => {
                        return Ok(ExtractionStats {
                            total_files,
                            total_bytes,
                            duration: Duration::default(),
                        });
                    }
                    CallbackAction::Continue => {}
                }
            }
        }

        match callback.on_file_end(&metadata)? {
            CallbackAction::Stop => break,
            _ => {}
        }

        total_files += 1;
    }

    Ok(ExtractionStats {
        total_files,
        total_bytes,
        duration: Duration::default(),
    })
}
