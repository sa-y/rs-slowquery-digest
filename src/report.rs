use crate::aggregator::QueryStats;
use crate::OutputFormat;
use std::collections::HashMap;
use std::path::PathBuf;
use std::io::Write;
use tabled::{Table, Tabled};

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "Rank")]
    rank: usize,
    #[tabled(rename = "Count")]
    count: u64,
    #[tabled(rename = "Total Time")]
    total_time: String,
    #[tabled(rename = "Mean Time")]
    mean_time: String,
    #[tabled(rename = "Query ID")]
    query_id: String,
    #[tabled(rename = "Query")]
    query: String,
}

#[derive(Debug)]
struct ReportItem {
    rank: usize,
    query_id: String,
    count: u64,
    total_time: f64,
    mean_time: f64,
    p95: f64,
    p99: f64,
    total_lock_time: f64,
    mean_lock_time: f64,
    rows_sent: u64,
    rows_examined: u64,
    ratio: f64,
    time_range: String,
    example_query: String,
    worst_example_query: String,
    normalized_query: String,
}

pub fn print_report(stats: HashMap<String, QueryStats>, format: OutputFormat, output_path: Option<&PathBuf>, timezone_str: &str, limit: usize) -> anyhow::Result<()> {
    let items = prepare_report_items(stats, timezone_str, limit);

    let mut writer: Box<dyn Write> = if let Some(path) = output_path {
        Box::new(std::fs::File::create(path)?)
    } else {
        Box::new(std::io::stdout())
    };

    match format {
        OutputFormat::Table => {
            // Always print the summary table first
            let rows: Vec<Row> = items.iter().map(|item| {
                let query_display = format_query(&item.example_query, &format);
                Row {
                    rank: item.rank,
                    count: item.count,
                    total_time: format!("{:.3}s", item.total_time),
                    mean_time: format!("{:.3}s", item.mean_time),
                    query_id: item.query_id.clone(),
                    query: query_display,
                }
            }).collect();

            print_table(rows, &mut writer)?;

            print_detailed_sections(&items, &mut writer)?;
        }
        OutputFormat::Html => {
            print_html(&items, &mut writer)?;
        }
    }
    Ok(())
}

fn prepare_report_items(stats: HashMap<String, QueryStats>, timezone_str: &str, limit: usize) -> Vec<ReportItem> {
    let mut stats_vec: Vec<(String, QueryStats)> = stats.into_iter().collect();
    
    // Sort by total time desc
    stats_vec.sort_by(|a, b| b.1.total_time.partial_cmp(&a.1.total_time).unwrap_or(std::cmp::Ordering::Equal));

    stats_vec.into_iter().enumerate().take(limit).map(|(i, (fp, mut stat))| {
        let digest = md5::compute(&fp);
        let query_id = format!("{:x}", digest);
        
        let mean = if stat.count > 0 { stat.total_time / stat.count as f64 } else { 0.0 };
        let mean_lock_time = if stat.count > 0 { stat.total_lock_time / stat.count as f64 } else { 0.0 };
        let ratio = if stat.total_rows_sent > 0 {
            stat.total_rows_examined as f64 / stat.total_rows_sent as f64
        } else {
            0.0
        };

        stat.all_query_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p95 = percentile(&stat.all_query_times, 0.95);
        let p99 = percentile(&stat.all_query_times, 0.99);

        let tz_offset = match timezone_str.parse::<chrono::FixedOffset>() {
            Ok(offset) => offset,
            Err(_) => {
                eprintln!("Warning: Invalid timezone offset '{}', using UTC.", timezone_str);
                chrono::FixedOffset::east_opt(0).unwrap()
            }
        };

        let time_range = if let (Some(first), Some(last)) = (stat.first_seen, stat.last_seen) {
            format!("{} - {}", first.with_timezone(&tz_offset).format("%Y-%m-%d %H:%M:%S %z"), last.with_timezone(&tz_offset).format("%Y-%m-%d %H:%M:%S %z"))
        } else {
            "N/A".to_string()
        };

        ReportItem {
            rank: i + 1,
            query_id,
            count: stat.count,
            total_time: stat.total_time,
            mean_time: mean,
            p95,
            p99,
            total_lock_time: stat.total_lock_time,
            mean_lock_time,
            rows_sent: stat.total_rows_sent,
            rows_examined: stat.total_rows_examined,
            ratio,
            time_range,
            example_query: stat.example_query,
            worst_example_query: stat.worst_example_query,
            normalized_query: fp,
        }
    }).collect()
}

fn print_detailed_sections(items: &[ReportItem], writer: &mut dyn Write) -> anyhow::Result<()> {
    writeln!(writer, "\nDetailed Report\n===============")?;
    
    for item in items {
        writeln!(writer, "\nQuery ID: {}", item.query_id)?;
        writeln!(writer, "Rank: {}", item.rank)?;
        writeln!(writer, "  Time Range: {}", item.time_range)?;
        writeln!(writer, "  Execution Stats:")?;
        writeln!(writer, "    Count: {}", item.count)?;
        writeln!(writer, "    Total Time: {:.3}s", item.total_time)?;
        writeln!(writer, "    Mean Time:  {:.3}s", item.mean_time)?;
        writeln!(writer, "    P95:        {:.3}s", item.p95)?;
        writeln!(writer, "    P99:        {:.3}s", item.p99)?;
        writeln!(writer, "    Total Lock Time: {:.3}s", item.total_lock_time)?;
        writeln!(writer, "    Mean Lock Time:  {:.3}s", item.mean_lock_time)?;
        writeln!(writer, "  Row Stats:")?;
        writeln!(writer, "    Sent:       {}", item.rows_sent)?;
        writeln!(writer, "    Examined:   {}", item.rows_examined)?;
        writeln!(writer, "    Examined/Sent Ratio: {:.2}", item.ratio)?;
        writeln!(writer, "  Normalized Query:")?;
        writeln!(writer, "    {}", item.normalized_query.trim())?;
        writeln!(writer, "  Worst Case Example:")?;
        writeln!(writer, "    {}", item.worst_example_query.trim())?;
        writeln!(writer, "--------------------------------------------------------------------------------")?;
    }
    Ok(())
}

fn print_html(items: &[ReportItem], writer: &mut dyn Write) -> anyhow::Result<()> {
    writeln!(writer, "<!DOCTYPE html>")?;
    writeln!(writer, "<html>")?;
    writeln!(writer, "<head>")?;
    writeln!(writer, "<title>Slow Query Digest Report</title>")?;
    writeln!(writer, "<style>")?;
    writeln!(writer, "body {{ font-family: sans-serif; margin: 20px; }}")?;
    writeln!(writer, "table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}")?;
    writeln!(writer, "th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}")?;
    writeln!(writer, "th {{ background-color: #f2f2f2; }}")?;
    writeln!(writer, ".query-block {{ border: 1px solid #ccc; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}")?;
    writeln!(writer, ".query-sql {{ background-color: #f8f8f8; padding: 10px; overflow-x: auto; font-family: monospace; }}")?;
    writeln!(writer, ".query-id {{ font-family: monospace; }}")?;
    writeln!(writer, ".copy-btn {{ margin-bottom: 5px; padding: 5px 10px; cursor: pointer; }}")?;
    writeln!(writer, "</style>")?;
    writeln!(writer, "<script>")?;
    writeln!(writer, "function copyToClipboard(elementId) {{")?;
    writeln!(writer, "  var copyText = document.getElementById(elementId).innerText;")?;
    writeln!(writer, "  navigator.clipboard.writeText(copyText).then(function() {{")?;
    writeln!(writer, "    alert('Copied to clipboard!');")?;
    writeln!(writer, "  }}, function(err) {{")?;
    writeln!(writer, "    console.error('Async: Could not copy text: ', err);")?;
    writeln!(writer, "  }});")?;
    writeln!(writer, "}}")?;
    writeln!(writer, "</script>")?;
    writeln!(writer, "</head>")?;
    writeln!(writer, "<body>")?;
    
    writeln!(writer, "<h1>Slow Query Digest Report</h1>")?;
    
    writeln!(writer, "<h2>Summary</h2>")?;
    writeln!(writer, "<table>")?;
    writeln!(writer, "<thead><tr><th>Rank</th><th>Count</th><th>Total Time</th><th>Mean Time</th><th>Query ID</th><th>Query</th></tr></thead>")?;
    writeln!(writer, "<tbody>")?;
    for item in items {
        let mut query_display = format_query(&item.example_query, &OutputFormat::Html);
        if query_display.len() > 100 {
            query_display.truncate(97);
            query_display.push_str("...");
        }
        writeln!(writer, "<tr>")?;
        writeln!(writer, "<td>{}</td>", item.rank)?;
        writeln!(writer, "<td>{}</td>", item.count)?;
        writeln!(writer, "<td>{:.3}s</td>", item.total_time)?;
        writeln!(writer, "<td>{:.3}s</td>", item.mean_time)?;
        writeln!(writer, "<td class=\"query-id\"><a href=\"#{}\">{}</a></td>", item.query_id, item.query_id)?;
        writeln!(writer, "<td>{}</td>", html_escape(&query_display))?;
        writeln!(writer, "</tr>")?;
    }
    writeln!(writer, "</tbody>")?;
    writeln!(writer, "</table>")?;

    writeln!(writer, "<h2>Detailed Report</h2>")?;
    for item in items {
        writeln!(writer, "<div id=\"{}\" class=\"query-block\">", item.query_id)?;
        writeln!(writer, "<h3>Rank {}: Query ID {}</h3>", item.rank, item.query_id)?;
        writeln!(writer, "<p><strong>Time Range:</strong> {}</p>", item.time_range)?;
        
        writeln!(writer, "<h4>Execution Stats</h4>")?;
        writeln!(writer, "<ul>")?;
        writeln!(writer, "<li>Count: {}</li>", item.count)?;
        writeln!(writer, "<li>Total Time: {:.3}s</li>", item.total_time)?;
        writeln!(writer, "<li>Mean Time: {:.3}s</li>", item.mean_time)?;
        writeln!(writer, "<li>P95: {:.3}s</li>", item.p95)?;
        writeln!(writer, "<li>P99: {:.3}s</li>", item.p99)?;
        writeln!(writer, "<li>Total Lock Time: {:.3}s</li>", item.total_lock_time)?;
        writeln!(writer, "<li>Mean Lock Time: {:.3}s</li>", item.mean_lock_time)?;
        writeln!(writer, "</ul>")?;

        writeln!(writer, "<h4>Row Stats</h4>")?;
        writeln!(writer, "<ul>")?;
        writeln!(writer, "<li>Sent: {}</li>", item.rows_sent)?;
        writeln!(writer, "<li>Examined: {}</li>", item.rows_examined)?;
        writeln!(writer, "<li>Examined/Sent Ratio: {:.2}</li>", item.ratio)?;
        writeln!(writer, "</ul>")?;

        writeln!(writer, "<h4>Normalized Query</h4>")?;
        writeln!(writer, "<button class=\"copy-btn\" onclick=\"copyToClipboard('norm-sql-{}')\">Copy SQL</button>", item.query_id)?;
        writeln!(writer, "<div class=\"query-sql\"><pre id=\"norm-sql-{}\">{}</pre></div>", item.query_id, html_escape(item.normalized_query.trim()))?;

        writeln!(writer, "<h4>Worst Case Example</h4>")?;
        writeln!(writer, "<button class=\"copy-btn\" onclick=\"copyToClipboard('sql-{}')\">Copy SQL</button>", item.query_id)?;
        writeln!(writer, "<div class=\"query-sql\"><pre id=\"sql-{}\">{}</pre></div>", item.query_id, html_escape(item.worst_example_query.trim()))?;
        
        writeln!(writer, "<p><a href=\"#top\">Back to Top</a></p>")?;
        writeln!(writer, "</div>")?;
    }

    writeln!(writer, "</body>")?;
    writeln!(writer, "</html>")?;
    Ok(())
}

fn html_escape(s: &str) -> String {
    s.replace("&", "&amp;")
     .replace("<", "&lt;")
     .replace(">", "&gt;")
     .replace("\"", "&quot;")
     .replace("'", "&#39;")
}

fn percentile(times: &[f64], p: f64) -> f64 {
    if times.is_empty() {
        return 0.0;
    }
    let idx = (times.len() as f64 * p).ceil() as usize;
    let idx = if idx == 0 { 0 } else { idx - 1 };
    times[idx.min(times.len() - 1)]
}

fn format_query(query: &str, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Table => {
             let mut q = query.replace("\n", " ");
             if q.len() > 50 {
                 q.truncate(47);
                 q.push_str("...");
             }
             q
        },
        _ => query.replace("\n", " "),
    }
}

fn print_table(rows: Vec<Row>, writer: &mut dyn Write) -> anyhow::Result<()> {
    let table = Table::new(rows).to_string();
    writeln!(writer, "{}", table)?;
    Ok(())
}
