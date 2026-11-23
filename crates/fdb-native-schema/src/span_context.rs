//! `SpanContext`: the `SPAN_PARENT` transaction option value (binary format) and the span
//! context of every request (flatbuffers).

use fdb_native_wire::binary::{
    Decode, DecodeError, Encode, EncodeError, ReadVersioning, Reader, Versioning, Writer,
    from_slice, to_vec,
};
use fdb_native_wire::{ProtocolVersion, Shape, Uid, Wire};

/// `TraceFlags` (`fdbclient/Tracing.h`): the flag byte of a [`SpanContext`]. C++ declares it
/// as `enum class TraceFlags : uint8_t` used as a bit field, so any byte is a value; bit 0
/// is `sampled`. This type holds the byte as it is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TraceFlags(pub u8);

impl TraceFlags {
    /// `TraceFlags::unsampled`.
    pub const UNSAMPLED: TraceFlags = TraceFlags(0);
    /// `TraceFlags::sampled`.
    pub const SAMPLED: TraceFlags = TraceFlags(1);

    /// Whether the sampled bit is set.
    pub const fn is_sampled(self) -> bool {
        self.0 & Self::SAMPLED.0 != 0
    }
}

impl Encode for TraceFlags {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
        w.write_u8(self.0)
    }
}

impl<'de> Decode<'de> for TraceFlags {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
        r.read_u8().map(TraceFlags)
    }
}

/// `SpanContext` (`fdbclient/Tracing.h`): a trace id, a span id and flags, serialized as
/// `serializer(ar, traceID, spanID, m_Flags)`.
///
/// It has both wire forms, identical on 7.4 and 8.0: in the binary format (the
/// `SPAN_PARENT` option value, see [`SpanContext::to_option_value`]) it is 25 bytes; in a
/// flatbuffers message (the `spanContext` of every request) it is a table of those three
/// fields. [`Default`] is `SpanContext()`: all zero, unsampled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Wire)]
pub struct SpanContext {
    /// `traceID`.
    pub trace_id: Uid,
    /// `spanID`.
    pub span_id: u64,
    /// `m_Flags`.
    pub flags: TraceFlags,
}

impl SpanContext {
    /// The length of a `SPAN_PARENT` transaction option value: an `IncludeVersion` prefix
    /// and the 25 bytes of the span context. (FDB rejects other lengths with
    /// `invalid_option_value`.)
    pub const OPTION_VALUE_LEN: usize = 33;

    /// The value of the `SPAN_PARENT` transaction option: `BinaryWriter::toValue(span,
    /// IncludeVersion(version))`. `version` is the protocol version of the process that
    /// writes it; the receiver only checks that it is valid.
    pub fn to_option_value(&self, version: ProtocolVersion) -> Result<Vec<u8>, EncodeError> {
        to_vec(self, Versioning::Include(version))
    }

    /// Reads the value of a `SPAN_PARENT` transaction option.
    pub fn from_option_value(value: &[u8]) -> Result<SpanContext, DecodeError> {
        if value.len() != Self::OPTION_VALUE_LEN {
            return Err(DecodeError::Invalid("SPAN_PARENT value is not 33 bytes"));
        }
        from_slice(value, ReadVersioning::INCLUDE_8_0)
    }
}

impl Encode for SpanContext {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
        w.write(&self.trace_id)?;
        w.write(&self.span_id)?;
        w.write(&self.flags)
    }
}

impl<'de> Decode<'de> for SpanContext {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
        Ok(SpanContext {
            trace_id: r.read()?,
            span_id: r.read()?,
            flags: r.read()?,
        })
    }
}

// The flatbuffers form of the flags: a `uint8_t` enum, a 1-byte scalar.
impl Wire for TraceFlags {
    const SHAPE: Shape = <u8 as Wire>::SHAPE;
}

impl fdb_native_wire::Encode for TraceFlags {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), fdb_native_wire::EncodeError> {
        fdb_native_wire::Encode::encode_inline(&self.0, out)
    }
}

impl<'de> fdb_native_wire::Decode<'de> for TraceFlags {
    fn absent() -> Self {
        TraceFlags::UNSAMPLED
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, fdb_native_wire::DecodeError> {
        <u8 as fdb_native_wire::Decode<'de>>::decode_inline(bytes).map(TraceFlags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatbuffers_table() {
        let span = SpanContext {
            trace_id: Uid::new(1, 2),
            span_id: 3,
            flags: TraceFlags(0x81),
        };
        let bytes = fdb_native_wire::encode_with_identifier(&span, 1).unwrap();
        let reader = fdb_native_wire::Reader::new(&bytes).unwrap();
        assert_eq!(reader.decode_root::<SpanContext>().unwrap(), span);
    }

    #[test]
    fn option_value() {
        let span = SpanContext {
            trace_id: Uid::new(1, 2),
            span_id: 3,
            flags: TraceFlags::SAMPLED,
        };
        let value = span.to_option_value(ProtocolVersion::V7_4).unwrap();
        assert_eq!(value.len(), SpanContext::OPTION_VALUE_LEN);
        assert_eq!(&value[..8], &ProtocolVersion::V7_4.0.to_le_bytes());
        assert_eq!(SpanContext::from_option_value(&value).unwrap(), span);
        assert!(SpanContext::from_option_value(&value[..32]).is_err());
    }

    #[test]
    fn any_flag_byte_is_a_value() {
        // The flags are a bit field in C++: bytes other than 0 and 1 are not errors.
        for byte in [0u8, 1, 2, 3, 0x80, 0xff] {
            let span = SpanContext {
                trace_id: Uid::new(9, 9),
                span_id: 9,
                flags: TraceFlags(byte),
            };
            let value = span.to_option_value(ProtocolVersion::V8_0).unwrap();
            let back = SpanContext::from_option_value(&value).unwrap();
            assert_eq!(back, span);
            assert_eq!(back.flags.is_sampled(), byte & 1 == 1);
        }
    }
}
