use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Tar parsing error: {0}")]
    TarError(String),

    #[error("Decompression error: {0}")]
    DecompressionError(String),

    #[error("File too large: {size} bytes (max: {max})")]
    FileTooLarge { size: u64, max: u64 },

    #[error("Callback error: {0}")]
    CallbackError(String),

    #[error("Invalid compression format")]
    InvalidCompression,

    #[error("URL parse error: {0}")]
    UrlParseError(String),
}

pub type Result<T> = std::result::Result<T, Error>;
