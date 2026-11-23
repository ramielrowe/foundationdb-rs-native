//! Unpacking: bytes to [`Element`]s.

use num_bigint::{BigInt, Sign};

use crate::MAX_DEPTH;
use crate::element::{Element, Uuid, Versionstamp};
use crate::encode::{
    BYTES, DOUBLE, FALSE, FLOAT, NEG_BIG_INT, NESTED, NULL, POS_BIG_INT, STRING, TRUE, UUID,
    VERSIONSTAMP,
};
use crate::error::{Error, Result};
use crate::tuple::Tuple;

/// Decodes a complete packed tuple. The whole input must be consumed.
pub(crate) fn decode(data: &[u8]) -> Result<Tuple> {
    let mut dec = Decoder { data, pos: 0 };
    let mut out = Vec::new();
    while dec.pos < data.len() {
        out.push(dec.element(0)?);
    }
    Ok(Tuple::from_elements(out))
}

struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Decoder<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.data.len())
            .ok_or(Error::UnexpectedEnd {
                offset: self.data.len(),
            })?;
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    /// Reads a null-terminated, `0x00 0xff`-escaped byte run.
    fn escaped(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let rest = &self.data[self.pos..];
            let Some(z) = rest.iter().position(|&b| b == 0) else {
                return Err(Error::UnexpectedEnd {
                    offset: self.data.len(),
                });
            };
            out.extend_from_slice(&rest[..z]);
            if rest.get(z + 1) == Some(&0xff) {
                out.push(0);
                self.pos += z + 2;
            } else {
                self.pos += z + 1;
                return Ok(out);
            }
        }
    }

    /// Decodes one element starting at its type code. `depth` is the nesting
    /// level of the tuple that contains it (0 at top level).
    fn element(&mut self, depth: usize) -> Result<Element> {
        let code_pos = self.pos;
        let code = self.take(1)?[0];
        Ok(match code {
            NULL => Element::Null,
            BYTES => Element::Bytes(self.escaped()?),
            STRING => {
                let bytes = self.escaped()?;
                Element::String(
                    String::from_utf8(bytes)
                        .map_err(|_| Error::InvalidUtf8 { offset: self.pos })?,
                )
            }
            NESTED => {
                if depth + 1 > MAX_DEPTH {
                    return Err(Error::NestingTooDeep);
                }
                let mut items = Vec::new();
                loop {
                    match self.data.get(self.pos) {
                        None => {
                            return Err(Error::UnexpectedEnd {
                                offset: self.data.len(),
                            });
                        }
                        Some(&NULL) => {
                            if self.data.get(self.pos + 1) == Some(&0xff) {
                                self.pos += 2;
                                items.push(Element::Null);
                            } else {
                                self.pos += 1;
                                break;
                            }
                        }
                        Some(_) => items.push(self.element(depth + 1)?),
                    }
                }
                Element::Tuple(Tuple::from_elements(items))
            }
            NEG_BIG_INT => {
                let len = usize::from(!self.take(1)?[0]);
                let mag: Vec<u8> = self.take(len)?.iter().map(|b| !b).collect();
                Element::from_bigint(BigInt::from_bytes_be(Sign::Minus, &mag))
            }
            POS_BIG_INT => {
                let len = usize::from(self.take(1)?[0]);
                let mag = self.take(len)?;
                Element::from_bigint(BigInt::from_bytes_be(Sign::Plus, mag))
            }
            0x0c..=0x1c => {
                let n = usize::from(code.abs_diff(0x14));
                let mut buf = [0u8; 8];
                buf[8 - n..].copy_from_slice(self.take(n)?);
                let raw = u64::from_be_bytes(buf);
                if code >= 0x14 {
                    match i64::try_from(raw) {
                        Ok(v) => Element::Int(v),
                        Err(_) => Element::BigInt(BigInt::from(raw)),
                    }
                } else {
                    let v = i128::from(raw) - ((1i128 << (8 * n)) - 1);
                    match i64::try_from(v) {
                        Ok(v) => Element::Int(v),
                        Err(_) => Element::BigInt(BigInt::from(v)),
                    }
                }
            }
            FLOAT => {
                let enc = u32::from_be_bytes(self.take_array()?);
                let bits = if enc >> 31 == 1 {
                    enc ^ 0x8000_0000
                } else {
                    !enc
                };
                Element::Float(f32::from_bits(bits))
            }
            DOUBLE => {
                let enc = u64::from_be_bytes(self.take_array()?);
                let bits = if enc >> 63 == 1 {
                    enc ^ 0x8000_0000_0000_0000
                } else {
                    !enc
                };
                Element::Double(f64::from_bits(bits))
            }
            FALSE => Element::Bool(false),
            TRUE => Element::Bool(true),
            UUID => Element::Uuid(Uuid::from_bytes(self.take_array()?)),
            VERSIONSTAMP => Element::Versionstamp(Versionstamp::from_bytes(self.take_array()?)),
            _ => {
                return Err(Error::UnknownTypeCode {
                    code,
                    offset: code_pos,
                });
            }
        })
    }
}
