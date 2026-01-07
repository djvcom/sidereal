//! JWT authentication middleware.

use axum::http::StatusCode;
use http::{Request, Response};
use jsonwebtoken::{decode, DecodingKey, TokenData, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

use crate::config::{AuthAlgorithm, AuthConfig};

/// Claims extracted from a valid JWT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    #[serde(default)]
    pub exp: Option<u64>,
    #[serde(default)]
    pub iat: Option<u64>,
    #[serde(default)]
    pub iss: Option<String>,
    #[serde(default)]
    pub aud: Option<String>,
}

/// Tower layer that validates JWT tokens in the Authorization header.
#[derive(Clone)]
pub struct AuthLayer {
    config: Arc<AuthConfig>,
}

impl AuthLayer {
    pub fn new(config: &AuthConfig) -> Self {
        Self {
            config: Arc::new(config.clone()),
        }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            config: self.config.clone(),
        }
    }
}

/// The service wrapper that performs JWT authentication.
#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    config: Arc<AuthConfig>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for AuthService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let config = self.config.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract Authorization header
            let auth_header = req
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok());

            let token = match auth_header {
                Some(header) if header.starts_with("Bearer ") => &header[7..],
                _ => {
                    tracing::debug!("Missing or invalid Authorization header");
                    return Ok(unauthorized_response());
                }
            };

            // Validate JWT
            match validate_token(token, &config) {
                Ok(token_data) => {
                    tracing::debug!(subject = %token_data.claims.sub, "JWT validated successfully");
                    inner.call(req).await
                }
                Err(e) => {
                    tracing::debug!(error = %e, "JWT validation failed");
                    Ok(unauthorized_response())
                }
            }
        })
    }
}

fn validate_token(
    token: &str,
    config: &AuthConfig,
) -> Result<TokenData<Claims>, jsonwebtoken::errors::Error> {
    let algorithm = match config.algorithm {
        AuthAlgorithm::HS256 => jsonwebtoken::Algorithm::HS256,
        AuthAlgorithm::HS384 => jsonwebtoken::Algorithm::HS384,
        AuthAlgorithm::HS512 => jsonwebtoken::Algorithm::HS512,
    };

    let mut validation = Validation::new(algorithm);

    // Configure issuer validation if specified (None = no validation)
    if let Some(ref issuer) = config.issuer {
        validation.set_issuer(&[issuer]);
    }

    // Configure audience validation if specified
    if let Some(ref audience) = config.audience {
        validation.set_audience(&[audience]);
    } else {
        // Disable audience validation if not configured
        validation.validate_aud = false;
    }

    let key = DecodingKey::from_secret(config.secret.as_bytes());
    decode::<Claims>(token, &key, &validation)
}

fn unauthorized_response<B: Default>() -> Response<B> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            http::header::WWW_AUTHENTICATE,
            "Bearer realm=\"sidereal-gateway\"",
        )
        .body(B::default())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use tower::ServiceExt;

    fn test_config() -> AuthConfig {
        AuthConfig {
            secret: "test-secret-key-for-testing-only".to_string(),
            algorithm: AuthAlgorithm::HS256,
            issuer: None,
            audience: None,
        }
    }

    fn create_token(claims: &Claims, secret: &str) -> String {
        let header = Header::new(jsonwebtoken::Algorithm::HS256);
        let key = EncodingKey::from_secret(secret.as_bytes());
        encode(&header, claims, &key).unwrap()
    }

    async fn test_service(_req: Request<Body>) -> Result<Response<Body>, std::convert::Infallible> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap())
    }

    #[tokio::test]
    async fn rejects_missing_auth_header() {
        let config = test_config();
        let layer = AuthLayer::new(&config);
        let service = layer.layer(tower::service_fn(test_service));

        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = service.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_invalid_token() {
        let config = test_config();
        let layer = AuthLayer::new(&config);
        let service = layer.layer(tower::service_fn(test_service));

        let req = Request::builder()
            .uri("/test")
            .header("Authorization", "Bearer invalid-token")
            .body(Body::empty())
            .unwrap();

        let response = service.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn accepts_valid_token() {
        let config = test_config();
        let claims = Claims {
            sub: "test-user".to_string(),
            exp: Some(u64::MAX),
            iat: None,
            iss: None,
            aud: None,
        };
        let token = create_token(&claims, &config.secret);

        let layer = AuthLayer::new(&config);
        let service = layer.layer(tower::service_fn(test_service));

        let req = Request::builder()
            .uri("/test")
            .header("Authorization", format!("Bearer {}", token))
            .body(Body::empty())
            .unwrap();

        let response = service.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn validates_issuer() {
        let mut config = test_config();
        config.issuer = Some("expected-issuer".to_string());

        let claims = Claims {
            sub: "test-user".to_string(),
            exp: Some(u64::MAX),
            iat: None,
            iss: Some("wrong-issuer".to_string()),
            aud: None,
        };
        let token = create_token(&claims, &config.secret);

        let layer = AuthLayer::new(&config);
        let service = layer.layer(tower::service_fn(test_service));

        let req = Request::builder()
            .uri("/test")
            .header("Authorization", format!("Bearer {}", token))
            .body(Body::empty())
            .unwrap();

        let response = service.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
