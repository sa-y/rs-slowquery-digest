use regex::Regex;
use std::sync::OnceLock;

static RE_NUMBER: OnceLock<Regex> = OnceLock::new();
static RE_STRING: OnceLock<Regex> = OnceLock::new();
static RE_WHITESPACE: OnceLock<Regex> = OnceLock::new();
static RE_COMMENT: OnceLock<Regex> = OnceLock::new();
static RE_USE: OnceLock<Regex> = OnceLock::new();

/// Generates a fingerprint for a SQL query by normalizing it.
///
/// Normalization includes:
/// - Removing `USE` statements
/// - Removing comments
/// - Replacing strings and numbers with `?`
/// - Collapsing whitespace
/// - Converting to lowercase
pub fn fingerprint(sql: &str) -> String {
    let re_number = RE_NUMBER.get_or_init(|| Regex::new(r"\b\d+\b").unwrap());
    let re_string = RE_STRING.get_or_init(|| Regex::new(r"'(?:[^']|'')*'").unwrap()); // Simple string regex
    let re_whitespace = RE_WHITESPACE.get_or_init(|| Regex::new(r"\s+").unwrap());
    let re_comment = RE_COMMENT.get_or_init(|| Regex::new(r"(?s:/\*.*?\*/)|--[^\n]*").unwrap());
    let re_use = RE_USE.get_or_init(|| Regex::new(r"(?i)use\s+\S+;").unwrap());

    // 0. Remove 'use <db>;' statements
    let no_use = re_use.replace_all(sql, "");

    // 1. Remove comments
    let no_comments = re_comment.replace_all(&no_use, "");

    // 2. Replace strings with ?
    let no_strings = re_string.replace_all(&no_comments, "?");

    // 3. Replace numbers with ?
    let no_numbers = re_number.replace_all(&no_strings, "?");

    // 4. Collapse whitespace
    let normalized = re_whitespace.replace_all(&no_numbers, " ").trim().to_string();

    normalized.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_basic() {
        let sql = "SELECT * FROM users WHERE id = 1";
        assert_eq!(fingerprint(sql), "select * from users where id = ?");
    }

    #[test]
    fn test_fingerprint_strings() {
        let sql = "SELECT * FROM users WHERE name = 'Alice'";
        assert_eq!(fingerprint(sql), "select * from users where name = ?");
    }

    #[test]
    fn test_fingerprint_numbers() {
        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        assert_eq!(fingerprint(sql), "select * from users where id in (?, ?, ?)");
    }

    #[test]
    fn test_fingerprint_whitespace() {
        let sql = "SELECT    *   FROM   users";
        assert_eq!(fingerprint(sql), "select * from users");
    }

    #[test]
    fn test_fingerprint_comments() {
        let sql = "SELECT * FROM users /* comment */ WHERE id = 1 -- comment";
        assert_eq!(fingerprint(sql), "select * from users where id = ?");
    }
    
    #[test]
    fn test_fingerprint_use() {
        let sql = "use mydb; SELECT * FROM users";
        assert_eq!(fingerprint(sql), "select * from users");
    }

    #[test]
    fn test_fingerprint_use_case_insensitive() {
        let sql = "USE mydb; SELECT * FROM users";
        assert_eq!(fingerprint(sql), "select * from users");
    }

    #[test]
    fn test_fingerprint_multiline_sql() {
        let sql = "SELECT * FROM users\n WHERE\n name = 'Alice'\n AND age = 17";
        assert_eq!(fingerprint(sql), "select * from users where name = ? and age = ?");
    }
}
