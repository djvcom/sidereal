//! Reliability conditions.
//!
//! A reliability condition is a promise about a kind of request, stated in
//! the terms of whoever depends on it rather than of the services that
//! serve it: *crawler requests for a product page are told whether the
//! product is in stock, 99% of the time, within 800 ms*. Nothing has to
//! error or slow down for such a promise to be broken, which is why it is
//! defined over what requests carry (their attributes) rather than over
//! any one service's health.
//!
//! A condition answers five questions:
//!
//! 1. **Which requests** does it cover? ([`Selector`])
//! 2. **What does good look like**, and how quickly? ([`Success`])
//! 3. **How often may it fail?** ([`Target`])
//! 4. **What is the impact** when it does? ([`Impact`])
//! 5. **Who should be told?** ([`Recipient`])
//!
//! # A request is a whole trace
//!
//! The span that identifies a request (the edge, which sees the user
//! agent) is rarely the span that knows whether it went well (a service
//! several hops in, which knows what stock was returned). So every test is
//! asked of the whole trace: a trace passes a test if *any* of its spans
//! does, and a condition never needs to know which service said what.
//!
//! # Definitions are data, not queries
//!
//! A condition is a small structured document. People describe what they
//! want in their own words and an assistant may draft the document, but
//! what is stored, shown back, and evaluated is the document itself,
//! translated to SQL by [`sql`]. Evaluation involves no model and gives
//! the same answer every time.

pub mod api;
pub mod evaluate;
pub mod sql;
pub mod store;

use serde::{Deserialize, Serialize};

/// Longest attribute key or identifier accepted in a definition.
const MAX_NAME_BYTES: usize = 128;

/// Most tests one list may hold.
const MAX_TESTS: usize = 16;

/// Whose attribute a test reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// An attribute of a span.
    #[default]
    Span,
    /// An attribute of the resource (service instance) that emitted a span.
    Resource,
}

/// A value an attribute is compared with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// A boolean.
    Flag(bool),
    /// A number.
    Number(f64),
    /// Text.
    Text(String),
}

/// What a test asks of an attribute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", content = "value", rename_all = "snake_case")]
pub enum Check {
    /// The attribute is present, whatever its value.
    Exists,
    /// The attribute equals the value.
    Equals(Value),
    /// The attribute is present and differs from the value.
    NotEquals(Value),
    /// The attribute, as text, contains the value.
    Contains(String),
    /// The attribute, as text, starts with the value.
    StartsWith(String),
    /// The attribute, as a number, is greater than the value.
    GreaterThan(f64),
    /// The attribute, as a number, is at least the value.
    AtLeast(f64),
    /// The attribute, as a number, is less than the value.
    LessThan(f64),
    /// The attribute, as a number, is at most the value.
    AtMost(f64),
    /// The attribute is `true`.
    IsTrue,
    /// The attribute is `false`.
    IsFalse,
}

/// One question about an attribute, asked of every span in a trace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Test {
    /// The attribute key, for example `http.route`.
    pub attribute: String,
    /// Whose attribute it is.
    #[serde(default)]
    pub scope: Scope,
    /// What is asked of it.
    #[serde(flatten)]
    pub check: Check,
}

/// Which requests a condition covers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Selector {
    /// Tests that must each be passed by some span of the trace.
    #[serde(default)]
    pub all: Vec<Test>,
    /// Tests that no span of the trace may pass.
    #[serde(default)]
    pub none: Vec<Test>,
}

const fn default_true() -> bool {
    true
}

/// What a good request looks like.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Success {
    /// Tests that must each be passed by some span of the trace.
    #[serde(default)]
    pub all: Vec<Test>,
    /// Tests that no span of the trace may pass.
    #[serde(default)]
    pub none: Vec<Test>,
    /// Whether a span that ended in error makes the request bad.
    /// Defaults to true.
    #[serde(default = "default_true")]
    pub no_errors: bool,
    /// The longest the request may take, in milliseconds, measured on its
    /// entry span. Absent means speed is not part of the promise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub within_ms: Option<u64>,
}

impl Default for Success {
    fn default() -> Self {
        Self {
            all: Vec::new(),
            none: Vec::new(),
            no_errors: true,
            within_ms: None,
        }
    }
}

const fn default_window_days() -> u32 {
    30
}

const fn default_minimum_requests() -> u64 {
    100
}

/// How often a condition may fail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Target {
    /// The share of requests that must be good, as a percentage.
    pub percent: f64,
    /// The rolling period over which that share is measured, in days.
    #[serde(default = "default_window_days")]
    pub window_days: u32,
    /// The fewest requests the period must contain for a verdict. Below
    /// it the condition reports that there is not enough data, rather
    /// than passing or failing on a handful of requests.
    #[serde(default = "default_minimum_requests")]
    pub minimum_requests: u64,
}

/// How much it matters when a condition is not met.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Worth knowing about.
    Low,
    /// Worth fixing soon.
    #[default]
    Medium,
    /// Costing the business now.
    High,
    /// Stop and fix it.
    Critical,
}

/// What it means for the business when a condition is not met.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Impact {
    /// In the owner's own words, for example "Google disapproves our
    /// product listings and they stop appearing in Shopping results".
    #[serde(default)]
    pub statement: String,
    /// How much it matters.
    #[serde(default)]
    pub severity: Severity,
}

/// How a recipient is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    /// An email address.
    Email,
    /// A Slack channel or webhook.
    Slack,
    /// An HTTP endpoint that receives a JSON notification.
    Webhook,
}

/// Someone to tell when a condition is at risk or not met.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recipient {
    /// Who this is, for display.
    pub name: String,
    /// How they are reached.
    pub channel: Channel,
    /// Where: an address, channel name or URL, according to the channel.
    pub address: String,
}

/// A reliability condition. See the [module documentation](self).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    /// Stable identifier: lowercase letters, digits and hyphens.
    pub id: String,
    /// What the condition is called.
    pub name: String,
    /// A longer explanation, if one helps.
    #[serde(default)]
    pub description: String,
    /// Which requests it covers.
    pub requests: Selector,
    /// What a good request looks like.
    #[serde(default)]
    pub success: Success,
    /// How often it may fail.
    pub target: Target,
    /// What it means when it is not met.
    #[serde(default)]
    pub impact: Impact,
    /// Who is told.
    #[serde(default)]
    pub notify: Vec<Recipient>,
    /// Days after creation during which the condition is measured and
    /// shown but notifies nobody, so that a new condition can be watched
    /// before anyone is woken by it. Zero means it is live at once.
    #[serde(default)]
    pub trial_days: u32,
    /// Whether evaluation is suspended.
    #[serde(default)]
    pub paused: bool,
}

/// Why a definition was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Invalid(pub String);

fn invalid<T>(message: impl Into<String>) -> Result<T, Invalid> {
    Err(Invalid(message.into()))
}

impl Test {
    fn validate(&self) -> Result<(), Invalid> {
        let key = self.attribute.as_str();
        if key.is_empty() || key.len() > MAX_NAME_BYTES || key.chars().any(char::is_control) {
            return invalid(format!(
                "attribute key {key:?} must be 1 to {MAX_NAME_BYTES} bytes with no control characters"
            ));
        }
        let finite = |n: f64| {
            if n.is_finite() {
                Ok(())
            } else {
                invalid(format!("the number compared with {key:?} must be finite"))
            }
        };
        match &self.check {
            Check::Equals(Value::Number(n))
            | Check::NotEquals(Value::Number(n))
            | Check::GreaterThan(n)
            | Check::AtLeast(n)
            | Check::LessThan(n)
            | Check::AtMost(n) => finite(*n),
            Check::Contains(text) | Check::StartsWith(text) if text.is_empty() => {
                invalid(format!("the text compared with {key:?} must not be empty"))
            }
            _ => Ok(()),
        }
    }
}

fn validate_tests(what: &str, tests: &[Test]) -> Result<(), Invalid> {
    if tests.len() > MAX_TESTS {
        return invalid(format!("{what} may hold at most {MAX_TESTS} tests"));
    }
    tests.iter().try_for_each(Test::validate)
}

impl Selector {
    /// Check that the selector is well formed and selects something.
    ///
    /// # Errors
    ///
    /// Returns the reason it is not acceptable.
    pub fn validate(&self) -> Result<(), Invalid> {
        if self.all.is_empty() {
            return invalid(
                "a condition must say which requests it covers: `requests.all` needs at least one test",
            );
        }
        validate_tests("requests.all", &self.all)?;
        validate_tests("requests.none", &self.none)
    }
}

impl Success {
    /// Check that the definition of success is well formed and tests
    /// something.
    ///
    /// # Errors
    ///
    /// Returns the reason it is not acceptable.
    pub fn validate(&self) -> Result<(), Invalid> {
        validate_tests("success.all", &self.all)?;
        validate_tests("success.none", &self.none)?;
        if self.within_ms == Some(0) {
            return invalid("success.within_ms must be greater than zero");
        }
        if self.all.is_empty()
            && self.none.is_empty()
            && !self.no_errors
            && self.within_ms.is_none()
        {
            return invalid("a condition must say what success is: every request would pass");
        }
        Ok(())
    }
}

impl Condition {
    /// Check that the definition is complete and well formed.
    ///
    /// # Errors
    ///
    /// Returns the first reason it is not acceptable, worded for the
    /// person who wrote it.
    pub fn validate(&self) -> Result<(), Invalid> {
        let id_ok = !self.id.is_empty()
            && self.id.len() <= 64
            && self
                .id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !self.id.starts_with('-')
            && !self.id.ends_with('-');
        if !id_ok {
            return invalid(
                "id must be 1 to 64 lowercase letters, digits and hyphens, and not start or end with a hyphen",
            );
        }
        if self.name.trim().is_empty() {
            return invalid("name must not be empty");
        }
        self.requests.validate()?;
        self.success.validate()?;

        let percent = self.target.percent;
        if !(percent > 0.0 && percent < 100.0) {
            return invalid("target.percent must be above 0 and below 100");
        }
        if !(1..=90).contains(&self.target.window_days) {
            return invalid("target.window_days must be between 1 and 90");
        }
        if self.trial_days > 90 {
            return invalid("trial_days must be at most 90");
        }
        for recipient in &self.notify {
            if recipient.name.trim().is_empty() || recipient.address.trim().is_empty() {
                return invalid("each entry in notify needs a name and an address");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;

    /// The worked example from the module documentation, as someone
    /// would write or an assistant would draft it.
    pub(crate) const GOOGLEBOT: &str = r#"{
        "id": "googlebot-product-availability",
        "name": "Googlebot sees product availability",
        "requests": {
            "all": [
                {"attribute": "user_agent.original", "op": "contains", "value": "Googlebot"},
                {"attribute": "http.route", "op": "equals", "value": "/products/{id}"}
            ]
        },
        "success": {
            "all": [
                {"attribute": "product.in_stock", "op": "exists"},
                {"attribute": "product.price", "op": "greater_than", "value": 0}
            ],
            "within_ms": 800
        },
        "target": {"percent": 99.0},
        "impact": {
            "statement": "Google disapproves our listings and they drop out of Shopping results",
            "severity": "high"
        },
        "notify": [
            {"name": "Search marketing", "channel": "email", "address": "search@example.com"}
        ],
        "trial_days": 7
    }"#;

    #[test]
    fn the_worked_example_parses_with_sensible_defaults() {
        let condition: Condition = serde_json::from_str(GOOGLEBOT).unwrap();
        condition.validate().unwrap();

        assert_eq!(condition.requests.all.len(), 2);
        assert_eq!(condition.requests.all[0].scope, Scope::Span);
        assert_eq!(
            condition.requests.all[0].check,
            Check::Contains("Googlebot".to_owned())
        );
        assert_eq!(condition.success.all[0].check, Check::Exists);
        assert_eq!(condition.success.all[1].check, Check::GreaterThan(0.0));
        assert!(condition.success.no_errors, "errors count unless stated");
        assert_eq!(condition.success.within_ms, Some(800));
        assert_eq!(condition.target.window_days, 30);
        assert_eq!(condition.target.minimum_requests, 100);
        assert_eq!(condition.impact.severity, Severity::High);
        assert_eq!(condition.trial_days, 7);
        assert!(!condition.paused);
    }

    #[test]
    fn a_definition_survives_being_stored_and_read_back() {
        let condition: Condition = serde_json::from_str(GOOGLEBOT).unwrap();
        let stored = serde_json::to_string(&condition).unwrap();
        let read: Condition = serde_json::from_str(&stored).unwrap();
        assert_eq!(read, condition);
    }

    #[test]
    fn values_keep_their_types() {
        let tests: Vec<Test> = serde_json::from_str(
            r#"[
                {"attribute": "a", "op": "equals", "value": "text"},
                {"attribute": "b", "op": "equals", "value": 200},
                {"attribute": "c", "op": "equals", "value": true},
                {"attribute": "d", "scope": "resource", "op": "is_false"}
            ]"#,
        )
        .unwrap();
        assert_eq!(
            tests[0].check,
            Check::Equals(Value::Text("text".to_owned()))
        );
        assert_eq!(tests[1].check, Check::Equals(Value::Number(200.0)));
        assert_eq!(tests[2].check, Check::Equals(Value::Flag(true)));
        assert_eq!(
            (tests[3].scope, &tests[3].check),
            (Scope::Resource, &Check::IsFalse)
        );
    }

    fn example() -> Condition {
        serde_json::from_str(GOOGLEBOT).unwrap()
    }

    fn rejected(change: impl FnOnce(&mut Condition)) -> String {
        let mut condition = example();
        change(&mut condition);
        condition.validate().unwrap_err().0
    }

    #[test]
    fn incomplete_or_malformed_definitions_are_rejected_with_a_reason() {
        assert!(rejected(|c| c.id = "Has Spaces".to_owned()).contains("id must be"));
        assert!(rejected(|c| c.id = "-leading".to_owned()).contains("id must be"));
        assert!(rejected(|c| c.name = "  ".to_owned()).contains("name"));
        assert!(rejected(|c| c.requests.all.clear()).contains("which requests"));
        assert!(rejected(|c| c.target.percent = 100.0).contains("percent"));
        assert!(rejected(|c| c.target.percent = 0.0).contains("percent"));
        assert!(rejected(|c| c.target.window_days = 0).contains("window_days"));
        assert!(rejected(|c| c.success.within_ms = Some(0)).contains("within_ms"));
        assert!(rejected(|c| c.requests.all[0].attribute = String::new()).contains("attribute key"));
        assert!(rejected(|c| c.success.all[1].check = Check::AtMost(f64::NAN)).contains("finite"));
        assert!(rejected(|c| {
            c.success = Success {
                no_errors: false,
                ..Success::default()
            };
        })
        .contains("what success is"));
        assert!(rejected(|c| c.notify[0].address = String::new()).contains("notify"));
    }

    #[test]
    fn success_may_rest_on_errors_or_speed_alone() {
        let mut condition = example();
        condition.success = Success::default();
        condition.validate().unwrap();

        condition.success = Success {
            no_errors: false,
            within_ms: Some(500),
            ..Success::default()
        };
        condition.validate().unwrap();
    }
}
