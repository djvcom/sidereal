//! Translation of a reliability condition into SQL.
//!
//! Every query starts from the same two steps:
//!
//! 1. `per_trace`: one row per trace in the period, recording for each
//!    test whether *any* span of the trace passed it, plus the trace's
//!    start, its duration and whether any span ended in error.
//! 2. `matched`: the traces the condition covers, each marked good or not.
//!
//! What differs between queries is only what is read from `matched`: a
//! total, a time series, examples, or the services involved.
//!
//! Attribute keys and values come from stored definitions, which people
//! write. They are only ever embedded as quoted string literals or as
//! numbers formatted here, never as SQL text.

use std::fmt::Write as _;

use chrono::{DateTime, Utc};

use super::{Check, Scope, Selector, Success, Test, Value};

/// A period of time: from `from` up to but not including `to`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// Start of the period.
    pub from: DateTime<Utc>,
    /// End of the period, exclusive.
    pub to: DateTime<Utc>,
}

impl Window {
    /// The period of `days` days ending at `to`.
    #[must_use]
    pub fn days_ending(to: DateTime<Utc>, days: u32) -> Self {
        Self {
            from: to - chrono::Duration::days(i64::from(days)),
            to,
        }
    }

    /// The period of `hours` hours ending at `to`.
    #[must_use]
    pub fn hours_ending(to: DateTime<Utc>, hours: u32) -> Self {
        Self {
            from: to - chrono::Duration::hours(i64::from(hours)),
            to,
        }
    }

    fn nanos(time: DateTime<Utc>) -> u64 {
        time.timestamp_nanos_opt()
            .and_then(|nanos| u64::try_from(nanos).ok())
            .unwrap_or(0)
    }

    /// Restriction of the `traces` table to this period.
    ///
    /// The `date` bounds repeat what the timestamps say so that whole
    /// partitions outside the period are skipped without being opened.
    fn predicate(&self) -> String {
        format!(
            "start_time_unix_nano >= {} AND start_time_unix_nano < {} \
             AND date >= '{}' AND date <= '{}'",
            Self::nanos(self.from),
            Self::nanos(self.to),
            self.from.format("%Y-%m-%d"),
            self.to.format("%Y-%m-%d"),
        )
    }
}

/// A string as a SQL literal.
fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A finite number as a SQL literal.
fn number(value: f64) -> String {
    // Debug formatting always includes a decimal point or exponent, so
    // the literal is read as a float.
    format!("{value:?}")
}

/// SQL that is true for a span passing `test`, and false or null
/// otherwise.
///
/// Attributes are compared as text or as numbers according to what the
/// test asks, whatever type they were sent with: an attribute sent as the
/// string `"200"` passes `equals 200`, and one sent as a number passes
/// `equals "200"`. A value that cannot be read as a number passes no
/// numeric test.
#[must_use]
pub fn test_sql(test: &Test) -> String {
    let function = match test.scope {
        Scope::Span => "attr",
        Scope::Resource => "resource_attr",
    };
    let raw = format!("{function}({})", quoted(&test.attribute));
    let as_text = format!("CAST({raw} AS VARCHAR)");
    let as_number = format!("TRY_CAST({raw} AS DOUBLE)");

    match &test.check {
        Check::Exists => format!("{raw} IS NOT NULL"),
        Check::Equals(Value::Text(value)) => format!("{as_text} = {}", quoted(value)),
        Check::NotEquals(Value::Text(value)) => format!("{as_text} <> {}", quoted(value)),
        Check::Equals(Value::Number(value)) => format!("{as_number} = {}", number(*value)),
        Check::NotEquals(Value::Number(value)) => format!("{as_number} <> {}", number(*value)),
        Check::Equals(Value::Flag(value)) => format!("{as_text} = '{value}'"),
        Check::NotEquals(Value::Flag(value)) => format!("{as_text} <> '{value}'"),
        Check::Contains(value) => format!("strpos({as_text}, {}) > 0", quoted(value)),
        Check::StartsWith(value) => format!("starts_with({as_text}, {})", quoted(value)),
        Check::GreaterThan(value) => format!("{as_number} > {}", number(*value)),
        Check::AtLeast(value) => format!("{as_number} >= {}", number(*value)),
        Check::LessThan(value) => format!("{as_number} < {}", number(*value)),
        Check::AtMost(value) => format!("{as_number} <= {}", number(*value)),
        Check::IsTrue => format!("{as_text} = 'true'"),
        Check::IsFalse => format!("{as_text} = 'false'"),
    }
}

/// The `WITH per_trace AS (...), matched AS (...)` prefix shared by every
/// query about a condition.
///
/// `matched` has one row per covered trace with the columns `trace_id`,
/// `started_ns`, `duration_ns` and `good`.
///
/// A request's duration is that of its entry span (the one with no
/// parent), which is how long the caller waited. Work that continues
/// afterwards, such as a queue consumer, does not count against it. If a
/// trace's entry span was not received, its longest span stands in.
#[must_use]
pub fn matched(requests: &Selector, success: &Success, window: &Window) -> String {
    let mut columns = String::new();
    let mut flag = |prefix: &str, tests: &[Test]| -> Vec<String> {
        tests
            .iter()
            .enumerate()
            .map(|(index, test)| {
                let name = format!("{prefix}{index}");
                let _ = write!(
                    columns,
                    ",\n    COALESCE(BOOL_OR({}), false) AS {name}",
                    test_sql(test)
                );
                name
            })
            .collect()
    };
    let covered_all = flag("covered_", &requests.all);
    let covered_none = flag("excluded_", &requests.none);
    let good_all = flag("passed_", &success.all);
    let good_none = flag("failed_", &success.none);

    let conjunction = |required: &[String], forbidden: &[String], extra: Vec<String>| {
        let terms: Vec<String> = required
            .iter()
            .cloned()
            .chain(forbidden.iter().map(|name| format!("NOT {name}")))
            .chain(extra)
            .collect();
        if terms.is_empty() {
            "true".to_owned()
        } else {
            terms.join(" AND ")
        }
    };

    let covered = conjunction(&covered_all, &covered_none, Vec::new());
    let mut extra = Vec::new();
    if success.no_errors {
        extra.push("NOT errored".to_owned());
    }
    if let Some(within_ms) = success.within_ms {
        extra.push(format!(
            "duration_ns <= {}",
            within_ms.saturating_mul(1_000_000)
        ));
    }
    let good = conjunction(&good_all, &good_none, extra);

    format!(
        "WITH per_trace AS (\n  SELECT trace_id,\n    \
         MIN(start_time_unix_nano) AS started_ns,\n    \
         COALESCE(MAX(duration_ns) FILTER (WHERE parent_span_id IS NULL), MAX(duration_ns)) AS duration_ns,\n    \
         COALESCE(BOOL_OR(status_code = 2), false) AS errored{columns}\n  \
         FROM traces\n  WHERE {}\n  GROUP BY trace_id\n),\n\
         matched AS (\n  SELECT trace_id, started_ns, duration_ns, ({good}) AS good\n  \
         FROM per_trace\n  WHERE {covered}\n)\n",
        window.predicate()
    )
}

/// One row: how many requests were covered, how many were good, and how
/// long they took (`mean_ns`, `p50_ns`, `p90_ns`, `p99_ns`).
#[must_use]
pub fn summary(requests: &Selector, success: &Success, window: &Window) -> String {
    format!(
        "{}SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE good) AS good, \
         AVG(duration_ns) AS mean_ns, \
         approx_percentile_cont(duration_ns, 0.5) AS p50_ns, \
         approx_percentile_cont(duration_ns, 0.9) AS p90_ns, \
         approx_percentile_cont(duration_ns, 0.99) AS p99_ns \
         FROM matched",
        matched(requests, success, window)
    )
}

/// One row per period of `bucket_secs` seconds that had covered requests:
/// `bucket` (its index since the epoch), `total` and `good`, in time
/// order.
#[must_use]
pub fn series(requests: &Selector, success: &Success, window: &Window, bucket_secs: u64) -> String {
    let bucket_ns = bucket_secs.max(1).saturating_mul(1_000_000_000);
    format!(
        "{}SELECT CAST(started_ns / {bucket_ns} AS BIGINT) AS bucket, \
         COUNT(*) AS total, COUNT(*) FILTER (WHERE good) AS good \
         FROM matched GROUP BY 1 ORDER BY 1",
        matched(requests, success, window)
    )
}

/// The most recent `limit` covered requests that were good, or that were
/// not: `trace_id`, `started_ns` and `duration_ns`.
#[must_use]
pub fn examples(
    requests: &Selector,
    success: &Success,
    window: &Window,
    good: bool,
    limit: usize,
) -> String {
    format!(
        "{}SELECT trace_id, started_ns, duration_ns FROM matched WHERE {} \
         ORDER BY started_ns DESC LIMIT {limit}",
        matched(requests, success, window),
        if good { "good" } else { "NOT good" },
    )
}

/// One row per service version that took part in covered requests:
/// `service`, `version`, `total` (covered requests it took part in) and
/// `bad` (those that were not good), busiest first.
///
/// Comparing a version's share of the bad requests with its share of all
/// of them is what points at the change responsible when a condition
/// stops being met.
#[must_use]
pub fn participants(requests: &Selector, success: &Success, window: &Window) -> String {
    format!(
        "{}SELECT s.\"service.name\" AS service, \
         COALESCE(s.\"service.version\", '') AS version, \
         COUNT(DISTINCT s.trace_id) AS total, \
         COUNT(DISTINCT CASE WHEN m.good THEN NULL ELSE s.trace_id END) AS bad \
         FROM traces s JOIN matched m ON s.trace_id = m.trace_id \
         WHERE {} \
         GROUP BY 1, 2 ORDER BY total DESC, service, version LIMIT 50",
        matched(requests, success, window),
        window.predicate()
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn test(attribute: &str, check: Check) -> Test {
        Test {
            attribute: attribute.to_owned(),
            scope: Scope::Span,
            check,
        }
    }

    #[test]
    fn each_check_compares_as_the_type_it_asks_for() {
        let sql = |check| test_sql(&test("k", check));
        assert_eq!(sql(Check::Exists), "attr('k') IS NOT NULL");
        assert_eq!(
            sql(Check::Equals(Value::Text("v".to_owned()))),
            "CAST(attr('k') AS VARCHAR) = 'v'"
        );
        assert_eq!(
            sql(Check::Equals(Value::Number(200.0))),
            "TRY_CAST(attr('k') AS DOUBLE) = 200.0"
        );
        assert_eq!(
            sql(Check::NotEquals(Value::Flag(false))),
            "CAST(attr('k') AS VARCHAR) <> 'false'"
        );
        assert_eq!(
            sql(Check::Contains("bot".to_owned())),
            "strpos(CAST(attr('k') AS VARCHAR), 'bot') > 0"
        );
        assert_eq!(
            sql(Check::AtMost(0.5)),
            "TRY_CAST(attr('k') AS DOUBLE) <= 0.5"
        );
        assert_eq!(sql(Check::IsTrue), "CAST(attr('k') AS VARCHAR) = 'true'");

        let resource = Test {
            scope: Scope::Resource,
            ..test("cloud.region", Check::Exists)
        };
        assert_eq!(
            test_sql(&resource),
            "resource_attr('cloud.region') IS NOT NULL"
        );
    }

    #[test]
    fn keys_and_values_cannot_escape_their_quotes() {
        let hostile = test(
            "k') OR 1=1 --",
            Check::Equals(Value::Text("'; DROP TABLE traces; --".to_owned())),
        );
        assert_eq!(
            test_sql(&hostile),
            "CAST(attr('k'') OR 1=1 --') AS VARCHAR) = '''; DROP TABLE traces; --'"
        );
    }

    #[test]
    fn the_period_bounds_both_timestamps_and_partitions() {
        let window = Window {
            from: Utc.with_ymd_and_hms(2026, 11, 1, 22, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 11, 2, 4, 0, 0).unwrap(),
        };
        let predicate = window.predicate();
        assert!(
            predicate.contains("start_time_unix_nano >= 1793570400000000000"),
            "{predicate}"
        );
        assert!(
            predicate.contains("start_time_unix_nano < 1793592000000000000"),
            "{predicate}"
        );
        assert!(
            predicate.contains("date >= '2026-11-01' AND date <= '2026-11-02'"),
            "{predicate}"
        );
    }

    #[test]
    fn coverage_and_success_are_judged_per_trace() {
        let requests = Selector {
            all: vec![test("http.route", Check::Exists)],
            none: vec![test("synthetic", Check::IsTrue)],
        };
        let success = Success {
            all: vec![test("product.in_stock", Check::Exists)],
            none: vec![test("fallback.used", Check::IsTrue)],
            no_errors: true,
            within_ms: Some(800),
        };
        let window = Window::hours_ending(Utc.with_ymd_and_hms(2026, 11, 2, 0, 0, 0).unwrap(), 1);
        let sql = matched(&requests, &success, &window);

        assert!(sql.contains("GROUP BY trace_id"), "{sql}");
        assert!(
            sql.contains("COALESCE(BOOL_OR(attr('http.route') IS NOT NULL), false) AS covered_0"),
            "{sql}"
        );
        assert!(sql.contains("WHERE covered_0 AND NOT excluded_0"), "{sql}");
        assert!(
            sql.contains(
                "(passed_0 AND NOT failed_0 AND NOT errored AND duration_ns <= 800000000) AS good"
            ),
            "{sql}"
        );
        assert!(
            sql.contains("FILTER (WHERE parent_span_id IS NULL)"),
            "{sql}"
        );
    }

    #[test]
    fn success_with_no_criteria_left_counts_every_request_as_good() {
        let requests = Selector {
            all: vec![test("http.route", Check::Exists)],
            none: Vec::new(),
        };
        let success = Success {
            no_errors: false,
            ..Success::default()
        };
        let window = Window::hours_ending(Utc::now(), 1);
        assert!(matched(&requests, &success, &window).contains("(true) AS good"));
    }
}
