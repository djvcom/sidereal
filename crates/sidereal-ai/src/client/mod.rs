//! HTTP client for the Sidereal query API.

use reqwest::StatusCode;
use serde_json::json;

/// Client for the Sidereal query API.
pub struct SiderealClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl SiderealClient {
    /// Create a new client targeting the given base URL with a Bearer token.
    pub fn new(base_url: String, token: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
            token,
        }
    }

    /// Check that the Sidereal service is reachable and healthy.
    pub async fn health(&self) -> Result<(), ClientError> {
        let url = format!("{}/health", self.base_url.trim_end_matches('/'));
        let response = self.http.get(&url).bearer_auth(&self.token).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            Err(ClientError::Api { status, body })
        }
    }

    /// Execute a SQL query and return the results as JSON.
    pub async fn sql(&self, query: &str) -> Result<serde_json::Value, ClientError> {
        let url = format!("{}/sql", self.base_url.trim_end_matches('/'));
        let request_body = json!({
            "sql": query,
            "format": "json"
        });

        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.token)
            .json(&request_body)
            .send()
            .await?;

        Self::json_or_error(response).await
    }

    /// Issue a GET request against a query API path, returning JSON.
    ///
    /// Parameters with `None` values are omitted from the query string.
    pub async fn get_json(
        &self,
        path: &str,
        params: Vec<(&str, Option<String>)>,
    ) -> Result<serde_json::Value, ClientError> {
        let url = format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        let query: Vec<(&str, String)> = params
            .into_iter()
            .filter_map(|(k, v)| v.map(|value| (k, value)))
            .collect();

        let response = self
            .http
            .get(&url)
            .query(&query)
            .bearer_auth(&self.token)
            .send()
            .await?;

        Self::json_or_error(response).await
    }

    async fn json_or_error(response: reqwest::Response) -> Result<serde_json::Value, ClientError> {
        if response.status() == StatusCode::OK {
            let value = response.json::<serde_json::Value>().await?;
            Ok(value)
        } else {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            Err(ClientError::Api { status, body })
        }
    }
}

/// Errors that can occur when communicating with the Sidereal API.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// An HTTP-level transport error occurred.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    /// The API returned a non-success status code.
    #[error("API error {status}: {body}")]
    Api {
        /// HTTP status code.
        status: u16,
        /// Response body from the API.
        body: String,
    },
}
