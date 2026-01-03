use regex::Regex;
use std::fs::File;
use std::io::Write as IoWrite;
use std::path::Path;
use tarstream::*;

struct SelectiveExtractor {
    pattern: Regex,
    output_dir: Option<String>,
    matched_files: Vec<String>,
    current_file: Option<File>,
    skip_current: bool,
    stop_on_first: bool,
    bytes_saved: u64,
    bytes_skipped: u64,
}

impl SelectiveExtractor {
    fn new(pattern: &str, output_dir: Option<String>, stop_on_first: bool) -> Result<Self> {
        let regex_pattern = glob_to_regex(pattern);
        let pattern = Regex::new(&regex_pattern)
            .map_err(|e| Error::CallbackError(format!("Invalid pattern: {}", e)))?;

        if let Some(ref dir) = output_dir {
            std::fs::create_dir_all(dir)?;
        }

        Ok(Self {
            pattern,
            output_dir,
            matched_files: Vec::new(),
            current_file: None,
            skip_current: false,
            stop_on_first,
            bytes_saved: 0,
            bytes_skipped: 0,
        })
    }
}

impl FileCallback for SelectiveExtractor {
    fn on_file_start(&mut self, metadata: &FileMetadata) -> Result<CallbackAction> {
        if metadata.is_directory {
            self.skip_current = true;
            return Ok(CallbackAction::Skip);
        }

        let is_match = self.pattern.is_match(&metadata.path);

        if self.matched_files.len() < 5 || is_match {
            if is_match {
                println!("✓ Matched: {} ({} bytes)", metadata.path, metadata.size);
            } else {
                println!("✗ Skipped: {}", metadata.path);
            }
        }

        if is_match {
            self.skip_current = false;

            if let Some(ref output_dir) = self.output_dir {
                let output_path = Path::new(output_dir).join(&metadata.path);

                if let Some(parent) = output_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                self.current_file = Some(File::create(output_path)?);
                self.matched_files.push(metadata.path.clone());
            }

            Ok(CallbackAction::Continue)
        } else {
            self.skip_current = true;
            self.bytes_skipped += metadata.size;
            Ok(CallbackAction::Skip)
        }
    }

    fn on_file_chunk(&mut self, chunk: &[u8]) -> Result<CallbackAction> {
        if !self.skip_current {
            self.bytes_saved += chunk.len() as u64;

            if let Some(ref mut file) = self.current_file {
                file.write_all(chunk)?;
            }
        }
        Ok(CallbackAction::Continue)
    }

    fn on_file_end(&mut self, _metadata: &FileMetadata) -> Result<CallbackAction> {
        self.current_file = None;

        if self.stop_on_first && !self.skip_current && !self.matched_files.is_empty() {
            println!("\n[stopping after first match]");
            return Ok(CallbackAction::Stop);
        }

        Ok(CallbackAction::Continue)
    }
}

fn glob_to_regex(pattern: &str) -> String {
    let pattern = if !pattern.contains('/') && !pattern.starts_with("**") {
        format!("**/{}", pattern)
    } else {
        pattern.to_string()
    };

    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '*' => {
                if chars.peek() == Some(&'*') {
                    chars.next();
                    regex.push_str(".*");
                } else {
                    regex.push_str("[^/]*");
                }
            }
            '?' => regex.push_str("[^/]"),
            '.' => regex.push_str("\\."),
            '+' => regex.push_str("\\+"),
            '(' => regex.push_str("\\("),
            ')' => regex.push_str("\\)"),
            '[' => regex.push_str("\\["),
            ']' => regex.push_str("\\]"),
            '{' => regex.push_str("\\{"),
            '}' => regex.push_str("\\}"),
            '^' => regex.push_str("\\^"),
            '$' => regex.push_str("\\$"),
            '|' => regex.push_str("\\|"),
            '\\' => regex.push_str("\\\\"),
            _ => regex.push(ch),
        }
    }

    regex.push('$');

    eprintln!("[DEBUG] Pattern: '{}' → Regex: '{}'", pattern, regex);
    regex
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <url> <pattern> [output_dir] [--stop-on-first]", args[0]);
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} https://example.com/archive.tar.gz '*.jpg' ./images", args[0]);
        eprintln!("  {} https://example.com/logs.tar.gz '*.json' ./filtered", args[0]);
        eprintln!("  {} https://example.com/data.tar.gz 'reports/**/*.csv' ./output", args[0]);
        eprintln!("  {} https://example.com/huge.tar.gz 'needle.txt' . --stop-on-first", args[0]);
        eprintln!();
        eprintln!("Pattern syntax:");
        eprintln!("  *     - matches any characters except /");
        eprintln!("  **    - matches any characters including /");
        eprintln!("  ?     - matches a single character");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --stop-on-first   stop extraction after finding first match");
        eprintln!();
        eprintln!("How it works:");
        eprintln!("  - Downloads and streams the tar file");
        eprintln!("  - Only saves files matching the pattern");
        eprintln!("  - Non-matching data is discarded from memory immediately");
        eprintln!("  - Memory efficient: processes 100GB archives without loading into RAM");
        std::process::exit(1);
    }

    let url = &args[1];
    let pattern = &args[2];

    let mut output_dir = None;
    let mut stop_on_first = false;

    for i in 3..args.len() {
        if args[i] == "--stop-on-first" {
            stop_on_first = true;
        } else if output_dir.is_none() {
            output_dir = Some(args[i].clone());
        }
    }

    println!("Downloading: {}", url);
    println!("Filtering for files matching: {}", pattern);
    if let Some(ref dir) = output_dir {
        println!("Output directory: {}", dir);
    } else {
        println!("Output directory: (none - analysis only)");
    }
    if stop_on_first {
        println!("Mode: stop after first match");
    }
    println!();

    let extractor = TarStreamExtractor::new()
        .with_compression(CompressionType::Auto)
        .with_buffer_size(64 * 1024);

    let mut callback = SelectiveExtractor::new(pattern, output_dir, stop_on_first)?;

    let stats = extractor.extract_from_url(url, &mut callback).await?;

    println!();
    println!("Extraction complete!");
    println!("  Total files in archive: {}", stats.total_files);
    println!("  Files matched: {}", callback.matched_files.len());
    println!("  Bytes saved: {} ({:.2} MB)",
        callback.bytes_saved,
        callback.bytes_saved as f64 / 1_024_000.0);
    println!("  Bytes skipped: {} ({:.2} MB)",
        callback.bytes_skipped,
        callback.bytes_skipped as f64 / 1_024_000.0);
    println!("  Memory efficiency: {:.1}% data discarded during streaming",
        (callback.bytes_skipped as f64 / (callback.bytes_saved + callback.bytes_skipped) as f64) * 100.0);
    println!("  Duration: {:?}", stats.duration);
    println!();

    if !callback.matched_files.is_empty() {
        println!("Matched files:");
        for path in &callback.matched_files {
            println!("  - {}", path);
        }
    }

    Ok(())
}
