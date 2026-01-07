//! Frame encoding and decoding utilities.

use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::error::ProtocolError;

/// Frame header size in bytes.
pub const FRAME_HEADER_SIZE: usize = 8;

/// Maximum message size (10 MB).
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Current protocol version.
pub const CURRENT_VERSION: u16 = crate::version::CURRENT;

/// Message type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum MessageType {
    /// Function invocation message.
    Function = 0x01,
    /// State operation message.
    State = 0x02,
    /// Control message.
    Control = 0x03,
}

impl MessageType {
    /// Creates a message type from a numeric value.
    #[must_use]
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x01 => Some(Self::Function),
            0x02 => Some(Self::State),
            0x03 => Some(Self::Control),
            _ => None,
        }
    }

    /// Returns the numeric value of this message type.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// Frame header for protocol messages.
///
/// Wire format (8 bytes, big-endian):
/// - Bytes 0-1: Protocol version (u16)
/// - Bytes 2-3: Message type (u16)
/// - Bytes 4-7: Payload length (u32)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Protocol version.
    pub version: u16,
    /// Message type discriminant.
    pub message_type: MessageType,
    /// Length of the payload in bytes.
    pub payload_len: u32,
}

impl FrameHeader {
    /// Creates a new frame header.
    #[must_use]
    pub const fn new(message_type: MessageType, payload_len: u32) -> Self {
        Self {
            version: CURRENT_VERSION,
            message_type,
            payload_len,
        }
    }

    /// Encodes the frame header to bytes.
    #[must_use]
    pub fn encode(&self) -> [u8; FRAME_HEADER_SIZE] {
        let mut buf = [0u8; FRAME_HEADER_SIZE];
        buf[0..2].copy_from_slice(&self.version.to_be_bytes());
        buf[2..4].copy_from_slice(&self.message_type.as_u16().to_be_bytes());
        buf[4..8].copy_from_slice(&self.payload_len.to_be_bytes());
        buf
    }

    /// Decodes a frame header from bytes.
    pub fn decode(bytes: &[u8; FRAME_HEADER_SIZE]) -> Result<Self, ProtocolError> {
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        let message_type_raw = u16::from_be_bytes([bytes[2], bytes[3]]);
        let payload_len = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        let message_type = MessageType::from_u16(message_type_raw)
            .ok_or(ProtocolError::UnknownMessageType(message_type_raw))?;

        Ok(Self {
            version,
            message_type,
            payload_len,
        })
    }

    /// Checks if this header's version is supported.
    #[must_use]
    pub fn is_version_supported(&self) -> bool {
        self.version >= crate::version::MIN_SUPPORTED && self.version <= crate::version::CURRENT
    }

    /// Validates the payload length.
    pub fn validate_payload_len(&self) -> Result<(), ProtocolError> {
        let len = self.payload_len as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge {
                size: len,
                max: MAX_MESSAGE_SIZE,
            });
        }
        Ok(())
    }
}

/// Codec for encoding and decoding protocol messages.
#[derive(Debug, Default)]
pub struct Codec {
    /// Reusable buffer for encoding.
    buffer: Vec<u8>,
}

impl Codec {
    /// Creates a new codec.
    #[must_use]
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Creates a codec with pre-allocated buffer capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
        }
    }

    /// Encodes an envelope to bytes (frame header + payload).
    ///
    /// Returns the complete frame including the 8-byte header.
    pub fn encode<T>(
        &mut self,
        envelope: &crate::Envelope<T>,
        message_type: MessageType,
    ) -> Result<&[u8], ProtocolError>
    where
        T: Archive,
        crate::Envelope<T>: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    {
        // Serialise envelope
        let payload =
            rkyv::to_bytes::<RkyvError>(envelope).map_err(|e| ProtocolError::Serialisation(e.to_string()))?;

        // Validate size
        if payload.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge {
                size: payload.len(),
                max: MAX_MESSAGE_SIZE,
            });
        }

        // Build frame
        let header = FrameHeader::new(message_type, payload.len() as u32);
        self.buffer.clear();
        self.buffer.extend_from_slice(&header.encode());
        self.buffer.extend_from_slice(&payload);

        Ok(&self.buffer)
    }

    /// Decodes an envelope from bytes.
    ///
    /// The bytes should NOT include the frame header - just the payload.
    pub fn decode<T>(bytes: &[u8]) -> Result<T, ProtocolError>
    where
        T: Archive,
        T::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
            + Deserialize<T, HighDeserializer<RkyvError>>,
    {
        rkyv::from_bytes::<T, RkyvError>(bytes).map_err(|e| ProtocolError::Deserialisation(e.to_string()))
    }

    /// Decodes an envelope from bytes without validation.
    ///
    /// # Safety
    ///
    /// The bytes must represent a valid archived `T`. Use only for trusted
    /// internal messages where the sender is known to be correct.
    pub unsafe fn decode_unchecked<T>(bytes: &[u8]) -> Result<T, ProtocolError>
    where
        T: Archive,
        T::Archived: Deserialize<T, HighDeserializer<RkyvError>>,
    {
        rkyv::from_bytes_unchecked::<T, RkyvError>(bytes)
            .map_err(|e| ProtocolError::Deserialisation(e.to_string()))
    }

    /// Returns the internal buffer for inspection.
    #[must_use]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_header_roundtrip() {
        let header = FrameHeader::new(MessageType::Function, 1024);
        let bytes = header.encode();
        let decoded = FrameHeader::decode(&bytes).unwrap();

        assert_eq!(header.version, decoded.version);
        assert_eq!(header.message_type, decoded.message_type);
        assert_eq!(header.payload_len, decoded.payload_len);
    }

    #[test]
    fn frame_header_version_check() {
        let header = FrameHeader::new(MessageType::State, 100);
        assert!(header.is_version_supported());

        let old_header = FrameHeader {
            version: 0,
            message_type: MessageType::State,
            payload_len: 100,
        };
        assert!(!old_header.is_version_supported());
    }

    #[test]
    fn frame_header_payload_validation() {
        let valid = FrameHeader::new(MessageType::Control, 1000);
        assert!(valid.validate_payload_len().is_ok());

        let too_large = FrameHeader::new(MessageType::Control, (MAX_MESSAGE_SIZE + 1) as u32);
        assert!(too_large.validate_payload_len().is_err());
    }

    #[test]
    fn message_type_roundtrip() {
        let types = [
            MessageType::Function,
            MessageType::State,
            MessageType::Control,
        ];

        for t in types {
            let value = t.as_u16();
            let restored = MessageType::from_u16(value);
            assert_eq!(restored, Some(t));
        }

        assert_eq!(MessageType::from_u16(0xFF), None);
    }

    #[test]
    fn codec_encode_decode() {
        use crate::{ControlMessage, Envelope};

        let mut codec = Codec::new();
        let envelope = Envelope::new(ControlMessage::Ping);

        let bytes = codec.encode(&envelope, MessageType::Control).unwrap();
        assert!(bytes.len() > FRAME_HEADER_SIZE);

        // Parse header
        let header_bytes: [u8; FRAME_HEADER_SIZE] = bytes[..FRAME_HEADER_SIZE].try_into().unwrap();
        let header = FrameHeader::decode(&header_bytes).unwrap();
        assert_eq!(header.message_type, MessageType::Control);

        // Decode payload
        let payload = &bytes[FRAME_HEADER_SIZE..];
        let decoded: Envelope<ControlMessage> = Codec::decode(payload).unwrap();
        assert_eq!(decoded.payload, ControlMessage::Ping);
    }
}
