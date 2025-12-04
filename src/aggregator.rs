use crate::parser::Query;
use crate::fingerprint::fingerprint;
use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// Aggregated statistics for a specific query fingerprint.
#[derive(Debug)]
pub struct QueryStats {
    pub count: u64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub total_lock_time: f64,
    pub total_rows_sent: u64,
    pub total_rows_examined: u64,
    pub example_query: String,
    pub all_query_times: Vec<f64>,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub worst_example_query: String,
}

impl Default for QueryStats {
    fn default() -> Self {
        Self {
            count: 0,
            total_time: 0.0,
            min_time: f64::MAX,
            max_time: 0.0,
            total_lock_time: 0.0,
            total_rows_sent: 0,
            total_rows_examined: 0,
            example_query: String::new(),
            all_query_times: Vec::new(),
            first_seen: None,
            last_seen: None,
            worst_example_query: String::new(),
        }
    }
}

/// Aggregates a stream of parsed queries into statistics grouped by fingerprint.
pub fn aggregate(queries: impl Iterator<Item = anyhow::Result<Query>>) -> HashMap<String, QueryStats> {
    let mut stats_map: HashMap<String, QueryStats> = HashMap::new();

    for query in queries.flatten() {
        let fp = fingerprint(&query.sql_text);
        let stats = stats_map.entry(fp).or_default();

        stats.count += 1;
        stats.total_time += query.query_time;
        if query.query_time < stats.min_time {
            stats.min_time = query.query_time;
        }
        if query.query_time > stats.max_time {
            stats.max_time = query.query_time;
            stats.worst_example_query = query.sql_text.clone();
        }
        stats.total_lock_time += query.lock_time;
        stats.total_rows_sent += query.rows_sent;
        stats.total_rows_examined += query.rows_examined;
        stats.all_query_times.push(query.query_time);

        if let Some(ts) = query.timestamp {
            if stats.first_seen.is_none() || ts < stats.first_seen.unwrap() {
                stats.first_seen = Some(ts);
            }
            if stats.last_seen.is_none() || ts > stats.last_seen.unwrap() {
                stats.last_seen = Some(ts);
            }
        }

        if stats.example_query.is_empty() {
            stats.example_query = query.sql_text;
        }
    }

    stats_map
}
