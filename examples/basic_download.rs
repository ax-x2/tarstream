use tarstream::*;
use std::fs::File;
use std::io::Write as IoWrite;
use std::path::Path;

struct FileExtractor {
    output_dir: String,
    current_file: Option<File>,
}

impl FileExtractor {
    fn new(output_dir: impl Into<String>) -> Self {
        Self {
            output_dir: output_dir.into(),
            current_file: None,
        }
    }
}

impl FileCallback for FileExtractor {
    fn on_file_start(&mut self, metadata: &FileMetadata) -> Result<CallbackAction> {
        println!("Extracting: {} ({} bytes)", metadata.path, metadata.size);

        if metadata.is_directory {
            let path = Path::new(&self.output_dir).join(&metadata.path);
            std::fs::create_dir_all(path)?;
            return Ok(CallbackAction::Continue);
        }

        let path = Path::new(&self.output_dir).join(&metadata.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        self.current_file = Some(File::create(path)?);
        Ok(CallbackAction::Continue)
    }

    fn on_file_chunk(&mut self, chunk: &[u8]) -> Result<CallbackAction> {
        if let Some(file) = &mut self.current_file {
            file.write_all(chunk)?;
        }
        Ok(CallbackAction::Continue)
    }

    fn on_file_end(&mut self, _metadata: &FileMetadata) -> Result<CallbackAction> {
        self.current_file = None;
        Ok(CallbackAction::Continue)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <url> [output_dir]", args[0]);
        eprintln!();
        eprintln!("Example:");
        eprintln!("  {} https://example.com/archive.tar.gz ./output", args[0]);
        std::process::exit(1);
    }

    let url = &args[1];
    let output_dir = if args.len() >= 3 {
        args[2].clone()
    } else {
        "./output".to_string()
    };

    println!("Downloading and extracting: {}", url);
    println!("Output directory: {}", output_dir);
    println!();

    let extractor = TarStreamExtractor::new()
        .with_compression(CompressionType::Auto)
        .with_buffer_size(64 * 1024);

    let mut callback = FileExtractor::new(output_dir);

    let stats = extractor.extract_from_url(url, &mut callback).await?;

    println!();
    println!("Extraction complete!");
    println!("  Files extracted: {}", stats.total_files);
    println!("  Total bytes: {}", stats.total_bytes);
    println!("  Duration: {:?}", stats.duration);

    Ok(())
}
