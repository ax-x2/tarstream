use crate::error::Result;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub path: String,
    pub size: u64,
    pub mode: u32,
    pub modified_time: Option<SystemTime>,
    pub is_directory: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackAction {
    Continue,
    Skip,
    Stop,
}

pub trait FileCallback: Send {
    fn on_file_start(&mut self, metadata: &FileMetadata) -> Result<CallbackAction>;

    fn on_file_chunk(&mut self, chunk: &[u8]) -> Result<CallbackAction>;

    fn on_file_end(&mut self, metadata: &FileMetadata) -> Result<CallbackAction>;

    fn on_error(&mut self, _error: &crate::error::Error) -> CallbackAction {
        CallbackAction::Continue
    }
}
