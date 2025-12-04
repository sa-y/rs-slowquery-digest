use anyhow::{Result};
use chrono::{DateTime, Utc};
use regex::Regex;
use std::io::BufRead;
use std::sync::OnceLock;

/// Represents a parsed slow query.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub query_time: f64,
    pub lock_time: f64,
    pub rows_sent: u64,
    pub rows_examined: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub user_host: String,
    pub sql_text: String,
}

static RE_HEADER_USER: OnceLock<Regex> = OnceLock::new();
static RE_HEADER_TIME: OnceLock<Regex> = OnceLock::new();
static RE_HEADER_METRICS: OnceLock<Regex> = OnceLock::new();

/// Parses a slow query log stream.
pub struct LogParser<R> {
    reader: R,
    current_block: String,
    read_buffer: String,
}

impl<R: BufRead> LogParser<R> {
    /// Creates a new `LogParser` for the given reader.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            current_block: String::new(),
            read_buffer: String::new(),
        }
    }

    /// Parses a single block of log lines into a `Query`.
    fn parse_block(&self, block: &str) -> Option<Query> {
        if block.is_empty() {
            return None;
        }

        let mut query_time = 0.0;
        let mut lock_time = 0.0;
        let mut rows_sent = 0;
        let mut rows_examined = 0;
        let mut user_host = String::new();
        let mut sql_lines = Vec::new();
        let mut timestamp = None;

        let re_header_user = RE_HEADER_USER.get_or_init(|| Regex::new(r"^# User@Host: (.*)").unwrap());
        let re_header_time = RE_HEADER_TIME.get_or_init(|| Regex::new(r"^# Time: (.*)").unwrap());
        let re_header_metrics = RE_HEADER_METRICS.get_or_init(|| Regex::new(r"Query_time: \s*([\d\.]+) \s*Lock_time: \s*([\d\.]+) \s*Rows_sent: \s*(\d+) \s*Rows_examined: \s*(\d+)").unwrap());

        for line in block.lines() {
            let trimmed = line.trim();
            if let Some(caps) = re_header_user.captures(trimmed) {
                user_host = caps[1].trim().to_string();
            } else if let Some(caps) = re_header_time.captures(trimmed) {
                let time_str = &caps[1];
                // Try parsing ISO 8601
                if let Ok(dt) = DateTime::parse_from_rfc3339(time_str) {
                    timestamp = Some(dt.with_timezone(&Utc));
                }
            } else if let Some(caps) = re_header_metrics.captures(trimmed) {
                query_time = caps[1].parse().unwrap_or(0.0);
                lock_time = caps[2].parse().unwrap_or(0.0);
                rows_sent = caps[3].parse().unwrap_or(0);
                rows_examined = caps[4].parse().unwrap_or(0);
            } else if trimmed.starts_with("#") {
                // Ignore other headers
            } else if trimmed.starts_with("SET timestamp=") {
                // Ignore for now
            } else {
                sql_lines.push(trimmed);
            }
        }

        let sql_text = sql_lines.join("\n").trim().to_string();
        if sql_text.is_empty() {
            return None;
        }

        Some(Query {
            query_time,
            lock_time,
            rows_sent,
            rows_examined,
            timestamp,
            user_host,
            sql_text,
        })
    }
    /// Checks if a block contains any SQL statements.
    fn has_sql(&self, block: &str) -> bool {
        for line in block.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("#") || trimmed.starts_with("SET timestamp=") {
                continue;
            }
            if !trimmed.is_empty() {
                return true;
            }
        }
        false
    }
}

impl<R: BufRead> Iterator for LogParser<R> {
    type Item = Result<Query>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.read_buffer.clear();
            match self.reader.read_line(&mut self.read_buffer) {
                Ok(0) => {
                    // EOF
                    if !self.current_block.is_empty() {
                        let q = self.parse_block(&self.current_block);
                        self.current_block.clear();
                        if let Some(query) = q {
                            return Some(Ok(query));
                        }
                    }
                    return None;
                }
                Ok(_) => {
                    // Continue processing
                }
                Err(e) => return Some(Err(anyhow::anyhow!(e))),
            }

            let trimmed = self.read_buffer.trim();
            
            // Heuristic: A new block often starts with # User@Host or # Time
            let is_header = trimmed.starts_with("# User@Host:") || trimmed.starts_with("# Time:");
            
            if is_header && self.has_sql(&self.current_block) {
                // We found a start of a NEW block, and we have data in current_block.
                // Process current_block as a query.
                let q = self.parse_block(&self.current_block);
                
                // Clear and start new block with this line
                self.current_block.clear();
                self.current_block.push_str(&self.read_buffer);
                
                if let Some(query) = q {
                    return Some(Ok(query));
                }
                // If previous block yielded no query (e.g. just headers?), continue loop
            } else {
                self.current_block.push_str(&self.read_buffer);
            }
        }
    }
}

/// Convenience function to create a `LogParser`.
pub fn parse_log<R: BufRead>(reader: R) -> LogParser<R> {
    LogParser::new(reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_block_standard() {
        let block = r#"# Time: 2023-10-27T10:00:00.123456Z
# User@Host: root[root] @ localhost []
# Query_time: 0.001234  Lock_time: 0.000123 Rows_sent: 10  Rows_examined: 100
SELECT * FROM users;"#;
        let parser = LogParser::new(&[][..]); // Dummy reader
        let query = parser.parse_block(block).unwrap();

        assert_eq!(query.query_time, 0.001234);
        assert_eq!(query.lock_time, 0.000123);
        assert_eq!(query.rows_sent, 10);
        assert_eq!(query.rows_examined, 100);
        assert_eq!(query.user_host, "root[root] @ localhost []");
        assert_eq!(query.sql_text, "SELECT * FROM users;");
        assert!(query.timestamp.is_some());
    }

    #[test]
    fn test_parse_block_multiline_sql() {
        let block = r#"# User@Host: root @ localhost
# Query_time: 1.0  Lock_time: 0.0 Rows_sent: 1  Rows_examined: 1
SELECT *
FROM users
WHERE id = 1;"#;
        let parser = LogParser::new(&[][..]);
        let query = parser.parse_block(block).unwrap();

        assert_eq!(query.sql_text, "SELECT *\nFROM users\nWHERE id = 1;");
    }

    #[test]
    fn test_parse_block_missing_header() {
        let block = "SELECT 1;";
        let parser = LogParser::new(&[][..]);
        let query = parser.parse_block(block).unwrap();

        assert_eq!(query.sql_text, "SELECT 1;");
        assert_eq!(query.query_time, 0.0);
    }
}
