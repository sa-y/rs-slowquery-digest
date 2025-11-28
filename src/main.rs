mod parser;
mod fingerprint;
mod aggregator;
mod report;

use clap::Parser;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the slow query log file(s)
    #[arg(long = "files", num_args = 1..)]
    files: Vec<PathBuf>,

    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,

    /// Output file path
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Timezone offset (e.g., "+09:00")
    #[arg(long, default_value = "+00:00")]
    timezone: String,

    /// Number of queries to show in the report
    #[arg(long, default_value_t = 20)]
    limit: usize,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum OutputFormat {
    Table,
    Html,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let readers: Vec<Box<dyn std::io::BufRead>> = if !args.files.is_empty() {
        let mut list = Vec::new();
        for path in args.files {
            match File::open(&path) {
                Ok(file) => {
                    list.push(Box::new(BufReader::new(file)) as Box<dyn std::io::BufRead>);
                }
                Err(e) => {
                    eprintln!("Warning: Could not open file {:?}: {}", path, e);
                }
            }
        }
        list
    } else {
        vec![Box::new(BufReader::new(io::stdin()))]
    };

    let parsers = readers.into_iter().map(parser::parse_log);
    let combined_parser = parsers.flatten();

    let stats = aggregator::aggregate(combined_parser);
    report::print_report(stats, args.format, args.output.as_ref(), &args.timezone, args.limit)?;

    Ok(())
}
