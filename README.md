# rs-slowquery-digest

`rs-slowquery-digest` is a command-line tool written in Rust for analyzing MySQL/MariaDB slow query logs. It aggregates query statistics and generates reports to help identify performance bottlenecks.

## Features

*   **Log Parsing**: Efficiently parses MySQL slow query logs.
*   **Aggregation**: Groups similar queries and calculates statistics (count, time, lock time, rows sent/examined).
*   **Multiple Output Formats**: Supports both text-based **Table** format and **HTML** reports.
*   **Timezone Support**: Allows specifying the timezone for the report.

## Installation

Ensure you have Rust and Cargo installed. Then, you can build and install the tool from source:

```bash
cargo install --path .
```

## Usage

```bash
rs-slowquery-digest [OPTIONS] [FILES]...
```

### Arguments

*   `[FILES]...`: Path to the slow query log file(s). If not provided, reads from standard input.

### Options


*   `--format <FORMAT>`: Output format. Values: `table` (default), `html`.
*   `-o, --output <OUTPUT>`: Output file path. If not specified, prints to stdout.
*   `--timezone <TIMEZONE>`: Timezone offset (e.g., "+09:00"). Default: "+00:00".
*   `--limit <LIMIT>`: Number of queries to show in the report. Default: 20.
*   `-h, --help`: Print help.
*   `-V, --version`: Print version.

## Examples

**1. Analyze a single log file and output a table to stdout:**

```bash
rs-slowquery-digest sample_logs/test_slow_01.log
```

**2. Analyze multiple files and generate an HTML report:**

```bash
rs-slowquery-digest --format html --output report.html sample_logs/test_slow_01.log sample_logs/test_multiline.log
```

**3. Analyze from stdin with a specific timezone and limit:**

```bash
cat sample_logs/test_slow_large.log | rs-slowquery-digest --timezone "+09:00" --limit 10
```

## License

[MIT License](LICENSE)
