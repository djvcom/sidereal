//! Rate limiting middleware using tower-governor.

use governor::middleware::NoOpMiddleware;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::SmartIpKeyExtractor;
use tower_governor::GovernorLayer;

use crate::config::RateLimitConfig;

/// The rate limit layer type used by the gateway.
pub type RateLimitLayer = GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, axum::body::Body>;

/// Create a rate limiting layer from configuration.
///
/// Uses `SmartIpKeyExtractor` which checks common reverse proxy headers
/// (x-forwarded-for, x-real-ip, forwarded) before falling back to peer IP.
pub fn create_rate_limit_layer(config: &RateLimitConfig) -> RateLimitLayer {
    let governor_config = GovernorConfigBuilder::default()
        .key_extractor(SmartIpKeyExtractor)
        .per_second(config.requests_per_second as u64)
        .burst_size(config.burst_size)
        .finish()
        .expect("Invalid rate limit configuration");

    GovernorLayer::new(governor_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_layer_from_config() {
        let config = RateLimitConfig {
            requests_per_second: 100,
            burst_size: 50,
        };

        let _layer = create_rate_limit_layer(&config);
    }

    #[test]
    fn creates_layer_with_low_limits() {
        let config = RateLimitConfig {
            requests_per_second: 1,
            burst_size: 1,
        };

        let _layer = create_rate_limit_layer(&config);
    }
}
