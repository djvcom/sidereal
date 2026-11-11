//! Measuring requests against a reliability condition.
//!
//! Three questions are answered here, all from stored telemetry:
//!
//! - [`profile`]: how do these requests behave today? Asked before a
//!   condition exists, to show its author what they would be promising.
//! - [`status`]: is the condition being met right now?
//! - [`backtest`]: would it have been met over the history we hold?
//!
//! Each answer comes with sentences in plain language as well as numbers,
//! because the people who own these conditions are not expected to read
//! percentiles.

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;

use super::sql::{self, Window};
use super::{Condition, Selector, Success};
use crate::query::QueryEngine;
use crate::TelemetryError;

/// Share of the failure allowance that, once used, puts a condition at
/// risk, as a percentage.
const AT_RISK_PERCENT: f64 = 75.0;

/// Targets a suggestion is chosen from, lowest first.
const TARGET_LADDER: [f64; 9] = [90.0, 95.0, 97.0, 98.0, 99.0, 99.5, 99.9, 99.95, 99.99];

/// How many recent examples of each kind a profile carries.
const EXAMPLES: usize = 5;

/// How long covered requests took, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Durations {
    /// The mean.
    pub mean_ms: f64,
    /// Half of requests were at least this fast.
    pub p50_ms: f64,
    /// Nine in ten were at least this fast.
    pub p90_ms: f64,
    /// Ninety-nine in a hundred were at least this fast.
    pub p99_ms: f64,
}

/// Totals for the requests a selector covers over a period.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Summary {
    /// Covered requests.
    pub total: u64,
    /// Those that were good.
    pub good: u64,
    /// `good` as a percentage of `total`; absent when there were none.
    pub success_percent: Option<f64>,
    /// How long they took; absent when there were none.
    pub durations: Option<Durations>,
}

/// Totals for one slice of a period.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Bucket {
    /// When the slice starts.
    pub start: DateTime<Utc>,
    /// Covered requests in it.
    pub total: u64,
    /// Those that were good.
    pub good: u64,
    /// `good` as a percentage of `total`.
    pub success_percent: f64,
}

/// One real request, to look at.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Example {
    /// Its trace ID, usable with `GET /traces/{trace_id}`.
    pub trace_id: String,
    /// When it started.
    pub started: DateTime<Utc>,
    /// How long it took, in milliseconds.
    pub duration_ms: f64,
}

/// A service version that took part in covered requests.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Participant {
    /// The service.
    pub service: String,
    /// Its version; empty if it does not report one.
    pub version: String,
    /// Covered requests it took part in.
    pub requests: u64,
    /// Those that were not good.
    pub failing: u64,
    /// Its share of all covered requests, as a percentage.
    pub share_of_requests_percent: f64,
    /// Its share of the covered requests that were not good, as a
    /// percentage. Far above its share of all requests means failures
    /// are concentrated in requests that pass through it.
    pub share_of_failures_percent: Option<f64>,
}

/// A target proposed from how the requests behave today.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Suggestion {
    /// A success target the requests are currently meeting, if there is
    /// one worth proposing.
    pub percent: Option<f64>,
    /// A time limit nearly all requests currently meet, in milliseconds.
    pub within_ms: Option<u64>,
    /// Why, in plain language.
    pub reasoning: Vec<String>,
}

/// How a kind of request behaves, for someone deciding what to promise
/// about it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Profile {
    /// Start of the period examined.
    pub from: DateTime<Utc>,
    /// End of the period examined.
    pub to: DateTime<Utc>,
    /// Totals over the period.
    pub summary: Summary,
    /// The same, hour by hour or day by day.
    pub series: Vec<Bucket>,
    /// Recent requests that were good.
    pub passing_examples: Vec<Example>,
    /// Recent requests that were not.
    pub failing_examples: Vec<Example>,
    /// The service versions involved.
    pub participants: Vec<Participant>,
    /// A target grounded in the figures above.
    pub suggestion: Suggestion,
    /// The findings in plain language.
    pub narrative: Vec<String>,
}

/// Whether a condition is being met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Met, with allowance to spare.
    Healthy,
    /// Met so far, but most of the failure allowance is used.
    AtRisk,
    /// Not met.
    Breached,
    /// Too few requests in the period to say.
    NotEnoughData,
    /// Evaluation is suspended.
    Paused,
}

/// A condition's standing over its rolling period.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Status {
    /// The verdict.
    pub state: State,
    /// Whether the condition is still in its trial, during which it is
    /// measured but notifies nobody.
    pub in_trial: bool,
    /// Start of the period measured.
    pub from: DateTime<Utc>,
    /// End of the period measured.
    pub to: DateTime<Utc>,
    /// Covered requests in the period.
    pub total: u64,
    /// Those that were good.
    pub good: u64,
    /// Those that were not.
    pub failures: u64,
    /// The share that were good, as a percentage.
    pub achieved_percent: Option<f64>,
    /// The share promised.
    pub target_percent: f64,
    /// How many requests the target allows to fail, given `total`.
    pub allowed_failures: u64,
    /// How much of that allowance is used, as a percentage. Above 100
    /// means the condition is not met.
    pub allowance_used_percent: Option<f64>,
    /// The verdict in one sentence.
    pub headline: String,
}

/// How a condition would have fared over stored history.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Backtest {
    /// Start of the history examined.
    pub from: DateTime<Utc>,
    /// End of the history examined.
    pub to: DateTime<Utc>,
    /// Totals over the whole of it.
    pub summary: Summary,
    /// Day by day.
    pub daily: Vec<Bucket>,
    /// Days that had covered requests.
    pub days_with_requests: usize,
    /// Of those, days on which the share of good requests was below the
    /// target.
    pub days_below_target: usize,
    /// The findings in plain language.
    pub narrative: Vec<String>,
}

fn percent(part: u64, whole: u64) -> Option<f64> {
    #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
    (whole > 0).then(|| part as f64 * 100.0 / whole as f64)
}

/// A column as floats, null where it is null or not numeric.
fn floats(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
    let mut values = Vec::new();
    for batch in batches {
        let Some(array) = batch
            .column_by_name(name)
            .and_then(|column| arrow::compute::cast(column, &DataType::Float64).ok())
        else {
            continue;
        };
        if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
            values.extend((0..array.len()).map(|i| (!array.is_null(i)).then(|| array.value(i))));
        }
    }
    values
}

/// A column as non-negative integers, zero where it is null.
fn counts(batches: &[RecordBatch], name: &str) -> Vec<u64> {
    let mut values = Vec::new();
    for batch in batches {
        let Some(array) = batch
            .column_by_name(name)
            .and_then(|column| arrow::compute::cast(column, &DataType::Int64).ok())
        else {
            continue;
        };
        if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
            values.extend((0..array.len()).map(|i| {
                if array.is_null(i) {
                    0
                } else {
                    u64::try_from(array.value(i)).unwrap_or(0)
                }
            }));
        }
    }
    values
}

/// A column as text, empty where it is null.
fn texts(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        let Some(array) = batch
            .column_by_name(name)
            .and_then(|column| arrow::compute::cast(column, &DataType::Utf8).ok())
        else {
            continue;
        };
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            values.extend((0..array.len()).map(|i| {
                if array.is_null(i) {
                    String::new()
                } else {
                    array.value(i).to_owned()
                }
            }));
        }
    }
    values
}

fn time_from_nanos(nanos: u64) -> DateTime<Utc> {
    i64::try_from(nanos)
        .ok()
        .map_or_else(Utc::now, |nanos| Utc.timestamp_nanos(nanos))
}

async fn summarise(
    engine: &QueryEngine,
    requests: &Selector,
    success: &Success,
    window: &Window,
) -> Result<Summary, TelemetryError> {
    let batches = engine
        .query(&sql::summary(requests, success, window))
        .await?;
    let total = counts(&batches, "total").first().copied().unwrap_or(0);
    let good = counts(&batches, "good").first().copied().unwrap_or(0);
    let millis = |name: &str| {
        floats(&batches, name)
            .first()
            .copied()
            .flatten()
            .map(|nanos| nanos / 1e6)
    };
    let durations = match (
        millis("mean_ns"),
        millis("p50_ns"),
        millis("p90_ns"),
        millis("p99_ns"),
    ) {
        (Some(mean_ms), Some(p50_ms), Some(p90_ms), Some(p99_ms)) if total > 0 => Some(Durations {
            mean_ms,
            p50_ms,
            p90_ms,
            p99_ms,
        }),
        _ => None,
    };
    Ok(Summary {
        total,
        good,
        success_percent: percent(good, total),
        durations,
    })
}

async fn buckets(
    engine: &QueryEngine,
    requests: &Selector,
    success: &Success,
    window: &Window,
    bucket_secs: u64,
) -> Result<Vec<Bucket>, TelemetryError> {
    let batches = engine
        .query(&sql::series(requests, success, window, bucket_secs))
        .await?;
    let starts = counts(&batches, "bucket");
    let totals = counts(&batches, "total");
    let goods = counts(&batches, "good");
    Ok(starts
        .into_iter()
        .zip(totals)
        .zip(goods)
        .map(|((bucket, total), good)| Bucket {
            start: time_from_nanos(
                bucket
                    .saturating_mul(bucket_secs)
                    .saturating_mul(1_000_000_000),
            ),
            total,
            good,
            success_percent: percent(good, total).unwrap_or(0.0),
        })
        .collect())
}

async fn examples(
    engine: &QueryEngine,
    requests: &Selector,
    success: &Success,
    window: &Window,
    good: bool,
) -> Result<Vec<Example>, TelemetryError> {
    let batches = engine
        .query(&sql::examples(requests, success, window, good, EXAMPLES))
        .await?;
    let mut ids = Vec::new();
    for batch in &batches {
        if let Some(array) = batch
            .column_by_name("trace_id")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        {
            ids.extend((0..array.len()).map(|i| hex::encode(array.value(i))));
        }
    }
    let starts = counts(&batches, "started_ns");
    let durations = floats(&batches, "duration_ns");
    Ok(ids
        .into_iter()
        .zip(starts)
        .zip(durations)
        .map(|((trace_id, started), duration)| Example {
            trace_id,
            started: time_from_nanos(started),
            duration_ms: duration.unwrap_or(0.0) / 1e6,
        })
        .collect())
}

async fn participants(
    engine: &QueryEngine,
    requests: &Selector,
    success: &Success,
    window: &Window,
    summary: &Summary,
) -> Result<Vec<Participant>, TelemetryError> {
    let batches = engine
        .query(&sql::participants(requests, success, window))
        .await?;
    let failures = summary.total.saturating_sub(summary.good);
    let rows = texts(&batches, "service")
        .into_iter()
        .zip(texts(&batches, "version"))
        .zip(counts(&batches, "total"))
        .zip(counts(&batches, "bad"));
    Ok(rows
        .map(|(((service, version), total), bad)| Participant {
            service,
            version,
            requests: total,
            failing: bad,
            share_of_requests_percent: percent(total, summary.total).unwrap_or(0.0),
            share_of_failures_percent: percent(bad, failures),
        })
        .collect())
}

/// A whole number with thousands separators.
fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A percentage with no more decimals than it needs.
fn pct(value: f64) -> String {
    let text = format!("{value:.2}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    format!("{text}%")
}

/// A duration in the unit a person would say it in.
fn spoken(millis: f64) -> String {
    if millis >= 10_000.0 {
        format!("{:.0} seconds", millis / 1000.0)
    } else if millis >= 1000.0 {
        format!("{:.1} seconds", millis / 1000.0)
    } else if millis >= 10.0 {
        format!("{millis:.0} ms")
    } else {
        format!("{millis:.1} ms")
    }
}

/// How long a period is, as a person would say it.
fn spoken_period(window: &Window) -> String {
    let hours = (window.to - window.from).num_hours();
    match hours {
        0..=1 => "hour".to_owned(),
        2..=47 => format!("{hours} hours"),
        _ => format!("{} days", hours / 24),
    }
}

/// Round up to two significant figures, so a suggested limit reads as a
/// number someone chose.
fn round_up(value: f64) -> u64 {
    if value <= 0.0 || !value.is_finite() {
        return 1;
    }
    let magnitude = 10f64.powf(value.log10().floor() - 1.0).max(1.0);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::as_conversions
    )]
    let rounded = ((value / magnitude).ceil() * magnitude) as u64;
    rounded.max(1)
}

fn suggest(summary: &Summary) -> Suggestion {
    let mut reasoning = Vec::new();
    let mut suggested_percent = None;
    let mut within_ms = None;

    match summary.success_percent {
        None => reasoning.push(
            "There were no matching requests in this period, so there is nothing to base a target on yet."
                .to_owned(),
        ),
        Some(current) => {
            suggested_percent = TARGET_LADDER
                .iter()
                .copied()
                .rev()
                .find(|candidate| *candidate < current);
            match suggested_percent {
                Some(target) => reasoning.push(format!(
                    "{} of these requests currently succeed, so {} is a target you are already meeting, with some room.",
                    pct(current),
                    pct(target)
                )),
                None => reasoning.push(format!(
                    "Only {} of these requests currently succeed. That is worth fixing before promising a target.",
                    pct(current)
                )),
            }
        }
    }

    if let Some(durations) = summary.durations {
        let limit = round_up(durations.p99_ms * 1.25);
        within_ms = Some(limit);
        #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
        reasoning.push(format!(
            "99 in 100 currently finish within {}, so a limit of {} would rarely be missed today.",
            spoken(durations.p99_ms),
            spoken(limit as f64)
        ));
    }

    Suggestion {
        percent: suggested_percent,
        within_ms,
        reasoning,
    }
}

fn describe(summary: &Summary, window: &Window, success: &Success) -> Vec<String> {
    let period = spoken_period(window);
    if summary.total == 0 {
        return vec![format!(
            "No requests matched in the last {period}. Either none were made, or the attributes used to pick them out are not being recorded."
        )];
    }
    let mut lines = vec![format!(
        "{} requests matched in the last {period}.",
        grouped(summary.total)
    )];
    if let Some(success_percent) = summary.success_percent {
        let failures = summary.total - summary.good;
        let judged_on_content = !success.all.is_empty() || !success.none.is_empty();
        lines.push(format!(
            "{} of them {}; {} did not.",
            pct(success_percent),
            if judged_on_content {
                "met your definition of success"
            } else {
                "completed without an error"
            },
            grouped(failures)
        ));
    }
    if let Some(durations) = summary.durations {
        lines.push(format!(
            "Half finished within {}, and 99 in 100 within {}.",
            spoken(durations.p50_ms),
            spoken(durations.p99_ms)
        ));
    }
    lines
}

/// Describe how the requests a selector covers behave over `window`.
///
/// # Errors
///
/// Returns an error if a query fails.
pub async fn profile(
    engine: &QueryEngine,
    requests: &Selector,
    success: &Success,
    window: Window,
) -> Result<Profile, TelemetryError> {
    let summary = summarise(engine, requests, success, &window).await?;
    let hours = (window.to - window.from).num_hours();
    let bucket_secs = if hours <= 48 { 3_600 } else { 86_400 };

    let (series, passing_examples, failing_examples, participants) = if summary.total == 0 {
        (Vec::new(), Vec::new(), Vec::new(), Vec::new())
    } else {
        (
            buckets(engine, requests, success, &window, bucket_secs).await?,
            examples(engine, requests, success, &window, true).await?,
            examples(engine, requests, success, &window, false).await?,
            participants(engine, requests, success, &window, &summary).await?,
        )
    };

    Ok(Profile {
        from: window.from,
        to: window.to,
        suggestion: suggest(&summary),
        narrative: describe(&summary, &window, success),
        summary,
        series,
        passing_examples,
        failing_examples,
        participants,
    })
}

/// How many of `total` requests a target of `target_percent` allows to
/// fail.
fn allowed_failures(total: u64, target_percent: f64) -> u64 {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::as_conversions
    )]
    let allowed = (total as f64 * (100.0 - target_percent) / 100.0).floor() as u64;
    allowed
}

/// Judge totals against a condition.
fn judge(condition: &Condition, summary: &Summary, window: &Window, in_trial: bool) -> Status {
    let total = summary.total;
    let failures = total.saturating_sub(summary.good);
    let target = condition.target.percent;
    let allowed = allowed_failures(total, target);
    #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
    let allowance_used = (total > 0).then(|| {
        let exact_allowance = total as f64 * (100.0 - target) / 100.0;
        failures as f64 * 100.0 / exact_allowance
    });
    let achieved = summary.success_percent;
    let period = spoken_period(window);

    let (state, headline) = if condition.paused {
        (
            State::Paused,
            "Paused: this condition is not being measured.".to_owned(),
        )
    } else if total < condition.target.minimum_requests {
        (
            State::NotEnoughData,
            format!(
                "Not enough data: {} matching requests in the last {period}, and at least {} are needed for a verdict.",
                grouped(total),
                grouped(condition.target.minimum_requests)
            ),
        )
    } else {
        let achieved = achieved.unwrap_or(0.0);
        let used = allowance_used.unwrap_or(0.0);
        if achieved < target {
            (
                State::Breached,
                format!(
                    "Not met: {} of requests succeeded in the last {period}, against a target of {}. {} failed; the target allows {}.",
                    pct(achieved),
                    pct(target),
                    grouped(failures),
                    grouped(allowed)
                ),
            )
        } else if used >= AT_RISK_PERCENT {
            (
                State::AtRisk,
                format!(
                    "At risk: {} of requests succeeded in the last {period}, but {} of the failure allowance is used ({} of {}).",
                    pct(achieved),
                    pct(used),
                    grouped(failures),
                    grouped(allowed)
                ),
            )
        } else {
            (
                State::Healthy,
                format!(
                    "Met: {} of requests succeeded in the last {period}, against a target of {}. {} of {} allowed failures used.",
                    pct(achieved),
                    pct(target),
                    grouped(failures),
                    grouped(allowed)
                ),
            )
        }
    };

    Status {
        state,
        in_trial,
        from: window.from,
        to: window.to,
        total,
        good: summary.good,
        failures,
        achieved_percent: achieved,
        target_percent: target,
        allowed_failures: allowed,
        allowance_used_percent: allowance_used,
        headline,
    }
}

/// Whether a condition created at `created` is still in its trial at
/// `now`.
#[must_use]
pub fn in_trial(condition: &Condition, created: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now < created + chrono::Duration::days(i64::from(condition.trial_days))
}

/// A condition's standing over its rolling period ending at `now`.
///
/// # Errors
///
/// Returns an error if the query fails.
pub async fn status(
    engine: &QueryEngine,
    condition: &Condition,
    created: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Status, TelemetryError> {
    let window = Window::days_ending(now, condition.target.window_days);
    let trial = in_trial(condition, created, now);
    if condition.paused {
        let nothing = Summary {
            total: 0,
            good: 0,
            success_percent: None,
            durations: None,
        };
        return Ok(judge(condition, &nothing, &window, trial));
    }
    let summary = summarise(engine, &condition.requests, &condition.success, &window).await?;
    Ok(judge(condition, &summary, &window, trial))
}

/// How a condition would have fared over the `days` days ending at `now`.
///
/// # Errors
///
/// Returns an error if a query fails.
pub async fn backtest(
    engine: &QueryEngine,
    condition: &Condition,
    now: DateTime<Utc>,
    days: u32,
) -> Result<Backtest, TelemetryError> {
    let window = Window::days_ending(now, days);
    let summary = summarise(engine, &condition.requests, &condition.success, &window).await?;
    let daily = if summary.total == 0 {
        Vec::new()
    } else {
        buckets(
            engine,
            &condition.requests,
            &condition.success,
            &window,
            86_400,
        )
        .await?
    };
    let target = condition.target.percent;
    let days_below_target = daily
        .iter()
        .filter(|day| day.success_percent < target)
        .count();

    let mut narrative = Vec::new();
    match summary.success_percent {
        None => narrative.push(format!(
            "There were no matching requests in the last {days} days, so there is no history to test against."
        )),
        Some(achieved) => {
            narrative.push(format!(
                "Over the last {days} days, {} of {} matching requests succeeded, against a target of {}.",
                pct(achieved),
                grouped(summary.total),
                pct(target)
            ));
            narrative.push(if days_below_target == 0 {
                format!(
                    "The target was met on each of the {} days that had requests.",
                    daily.len()
                )
            } else {
                format!(
                    "On {days_below_target} of the {} days that had requests, fewer than {} succeeded.",
                    daily.len(),
                    pct(target)
                )
            });
        }
    }

    Ok(Backtest {
        from: window.from,
        to: window.to,
        days_with_requests: daily.len(),
        days_below_target,
        summary,
        daily,
        narrative,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::float_cmp)]
mod tests {
    use super::*;
    use crate::conditions::tests::GOOGLEBOT;

    fn condition() -> Condition {
        serde_json::from_str(GOOGLEBOT).unwrap()
    }

    fn summary(total: u64, good: u64) -> Summary {
        Summary {
            total,
            good,
            success_percent: percent(good, total),
            durations: None,
        }
    }

    fn judged(total: u64, good: u64) -> Status {
        let window = Window::days_ending(Utc::now(), 30);
        judge(&condition(), &summary(total, good), &window, false)
    }

    #[test]
    fn numbers_are_written_the_way_people_say_them() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(12_430), "12,430");
        assert_eq!(grouped(1_000_000), "1,000,000");
        assert_eq!(pct(99.0), "99%");
        assert_eq!(pct(99.5), "99.5%");
        assert_eq!(pct(99.954), "99.95%");
        assert_eq!(spoken(4.26), "4.3 ms");
        assert_eq!(spoken(85.4), "85 ms");
        assert_eq!(spoken(1_240.0), "1.2 seconds");
        assert_eq!(spoken(42_000.0), "42 seconds");
        assert_eq!(round_up(643.0), 650);
        assert_eq!(round_up(81.2), 82);
        assert_eq!(round_up(1_201.0), 1_300);
        assert_eq!(round_up(7.3), 8);
    }

    #[test]
    fn a_condition_with_room_to_spare_is_healthy() {
        // 99% of 10,000 allows 100 failures; 20 used.
        let status = judged(10_000, 9_980);
        assert_eq!(status.state, State::Healthy);
        assert_eq!((status.failures, status.allowed_failures), (20, 100));
        assert_eq!(status.allowance_used_percent, Some(20.0));
        assert!(
            status
                .headline
                .starts_with("Met: 99.8% of requests succeeded"),
            "{}",
            status.headline
        );
        assert!(
            status.headline.contains("20 of 100 allowed failures"),
            "{}",
            status.headline
        );
    }

    #[test]
    fn most_of_the_allowance_used_is_at_risk_before_it_is_a_breach() {
        let status = judged(10_000, 9_920);
        assert_eq!(status.state, State::AtRisk);
        assert_eq!(status.allowance_used_percent, Some(80.0));
        assert!(
            status.headline.starts_with("At risk:"),
            "{}",
            status.headline
        );
    }

    #[test]
    fn falling_below_the_target_is_a_breach() {
        let status = judged(10_000, 9_850);
        assert_eq!(status.state, State::Breached);
        assert!(status.allowance_used_percent.unwrap() > 100.0);
        assert!(
            status
                .headline
                .contains("150 failed; the target allows 100."),
            "{}",
            status.headline
        );
    }

    #[test]
    fn too_few_requests_is_not_a_verdict_either_way() {
        // Every one of them failed, and it is still not a breach.
        let status = judged(40, 0);
        assert_eq!(status.state, State::NotEnoughData);
        assert!(
            status.headline.contains("40 matching requests"),
            "{}",
            status.headline
        );
        assert!(
            status.headline.contains("at least 100"),
            "{}",
            status.headline
        );
    }

    #[test]
    fn a_paused_condition_reports_only_that() {
        let mut paused = condition();
        paused.paused = true;
        let window = Window::days_ending(Utc::now(), 30);
        let status = judge(&paused, &summary(10_000, 0), &window, false);
        assert_eq!(status.state, State::Paused);
    }

    #[test]
    fn a_trial_lasts_the_stated_days_from_creation() {
        let condition = condition();
        let created = Utc.with_ymd_and_hms(2026, 11, 1, 9, 0, 0).unwrap();
        assert!(in_trial(
            &condition,
            created,
            created + chrono::Duration::days(6)
        ));
        assert!(!in_trial(
            &condition,
            created,
            created + chrono::Duration::days(7)
        ));

        let mut live = condition;
        live.trial_days = 0;
        assert!(!in_trial(&live, created, created));
    }

    #[test]
    fn a_suggested_target_is_one_already_being_met() {
        let mut current = summary(10_000, 9_963);
        current.durations = Some(Durations {
            mean_ms: 120.0,
            p50_ms: 85.0,
            p90_ms: 300.0,
            p99_ms: 640.0,
        });
        let suggestion = suggest(&current);
        assert_eq!(suggestion.percent, Some(99.5));
        assert_eq!(suggestion.within_ms, Some(800));
        assert!(suggestion.reasoning[0].contains("99.63% of these requests currently succeed"));

        // Exactly on a rung is not "with room": step down.
        assert_eq!(suggest(&summary(1_000, 990)).percent, Some(98.0));
        // Nothing sensible to promise yet.
        let poor = suggest(&summary(1_000, 700));
        assert_eq!(poor.percent, None);
        assert!(
            poor.reasoning[0].contains("worth fixing"),
            "{:?}",
            poor.reasoning
        );
        // And nothing to go on at all.
        let empty = suggest(&summary(0, 0));
        assert_eq!((empty.percent, empty.within_ms), (None, None));
    }

    #[test]
    fn an_empty_result_says_what_might_be_wrong() {
        let window = Window::days_ending(Utc::now(), 7);
        let lines = describe(&summary(0, 0), &window, &Success::default());
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("not being recorded"), "{}", lines[0]);
    }
}
