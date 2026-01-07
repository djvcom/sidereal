//! Common types used across the protocol.

use rkyv::{Archive, Deserialize, Serialize};

/// Correlation ID for request/response matching.
///
/// Uses ULID format (128-bit, lexicographically sortable, monotonic).
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[rkyv(compare(PartialEq))]
pub struct CorrelationId(pub [u8; 16]);

impl CorrelationId {
    /// Creates a new correlation ID from the current timestamp.
    #[must_use]
    pub fn new() -> Self {
        Self(ulid::Ulid::new().to_bytes())
    }

    /// Creates a correlation ID from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the raw bytes of this correlation ID.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Converts to a ULID for display purposes.
    #[must_use]
    pub fn to_ulid(&self) -> ulid::Ulid {
        ulid::Ulid::from_bytes(self.0)
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_ulid())
    }
}

impl From<ulid::Ulid> for CorrelationId {
    fn from(ulid: ulid::Ulid) -> Self {
        Self(ulid.to_bytes())
    }
}

impl From<CorrelationId> for ulid::Ulid {
    fn from(id: CorrelationId) -> Self {
        ulid::Ulid::from_bytes(id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_id_roundtrip() {
        let id = CorrelationId::new();
        let bytes = id.as_bytes();
        let restored = CorrelationId::from_bytes(*bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn correlation_id_display() {
        let id = CorrelationId::new();
        let display = id.to_string();
        // ULID is 26 characters
        assert_eq!(display.len(), 26);
    }

    #[test]
    fn correlation_id_ulid_conversion() {
        let ulid = ulid::Ulid::new();
        let id = CorrelationId::from(ulid);
        let back: ulid::Ulid = id.into();
        assert_eq!(ulid, back);
    }
}
