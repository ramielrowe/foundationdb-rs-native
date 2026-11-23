//! Packing: [`Element`]s to bytes.

use num_bigint::{BigInt, Sign};

use crate::MAX_DEPTH;
use crate::element::{Element, Versionstamp};
use crate::error::{Error, Result};

pub(crate) const NULL: u8 = 0x00;
pub(crate) const BYTES: u8 = 0x01;
pub(crate) const STRING: u8 = 0x02;
pub(crate) const NESTED: u8 = 0x05;
pub(crate) const NEG_BIG_INT: u8 = 0x0b;
pub(crate) const INT_ZERO: u8 = 0x14;
pub(crate) const POS_BIG_INT: u8 = 0x1d;
pub(crate) const FLOAT: u8 = 0x20;
pub(crate) const DOUBLE: u8 = 0x21;
pub(crate) const FALSE: u8 = 0x26;
pub(crate) const TRUE: u8 = 0x27;
pub(crate) const UUID: u8 = 0x30;
pub(crate) const VERSIONSTAMP: u8 = 0x33;

/// Result of encoding: the bytes plus the position of the single incomplete
/// versionstamp's transaction version, if one was allowed and present.
pub(crate) struct Encoded {
    pub(crate) bytes: Vec<u8>,
    pub(crate) incomplete_pos: Option<usize>,
}

struct Encoder {
    out: Vec<u8>,
    allow_incomplete: bool,
    incomplete_pos: Option<usize>,
}

/// Encodes `elements` after `prefix`. Positions reported include the prefix.
pub(crate) fn encode(
    prefix: &[u8],
    elements: &[Element],
    allow_incomplete: bool,
) -> Result<Encoded> {
    let mut enc = Encoder {
        out: prefix.to_vec(),
        allow_incomplete,
        incomplete_pos: None,
    };
    for e in elements {
        enc.element(e, false, 0)?;
    }
    Ok(Encoded {
        bytes: enc.out,
        incomplete_pos: enc.incomplete_pos,
    })
}

impl Encoder {
    fn element(&mut self, e: &Element, nested: bool, depth: usize) -> Result<()> {
        match e {
            Element::Null => {
                self.out.push(NULL);
                if nested {
                    self.out.push(0xff);
                }
            }
            Element::Bytes(b) => {
                self.out.push(BYTES);
                self.escaped(b);
            }
            Element::String(s) => {
                self.out.push(STRING);
                self.escaped(s.as_bytes());
            }
            Element::Tuple(t) => {
                if depth + 1 > MAX_DEPTH {
                    return Err(Error::NestingTooDeep);
                }
                self.out.push(NESTED);
                for inner in t.iter() {
                    self.element(inner, true, depth + 1)?;
                }
                self.out.push(NULL);
            }
            Element::Int(v) => self.int(*v),
            Element::BigInt(v) => self.big_int(v)?,
            Element::Float(f) => {
                self.out.push(FLOAT);
                let bits = f.to_bits();
                let enc = if bits >> 31 == 1 {
                    !bits
                } else {
                    bits ^ 0x8000_0000
                };
                self.out.extend_from_slice(&enc.to_be_bytes());
            }
            Element::Double(f) => {
                self.out.push(DOUBLE);
                let bits = f.to_bits();
                let enc = if bits >> 63 == 1 {
                    !bits
                } else {
                    bits ^ 0x8000_0000_0000_0000
                };
                self.out.extend_from_slice(&enc.to_be_bytes());
            }
            Element::Bool(b) => self.out.push(if *b { TRUE } else { FALSE }),
            Element::Uuid(u) => {
                self.out.push(UUID);
                self.out.extend_from_slice(u.as_bytes());
            }
            Element::Versionstamp(v) => self.versionstamp(v)?,
        }
        Ok(())
    }

    fn escaped(&mut self, bytes: &[u8]) {
        for chunk in bytes.split_inclusive(|&b| b == 0) {
            self.out.extend_from_slice(chunk);
            if chunk.last() == Some(&0) {
                self.out.push(0xff);
            }
        }
        self.out.push(NULL);
    }

    fn int(&mut self, v: i64) {
        if v == 0 {
            self.out.push(INT_ZERO);
            return;
        }
        let mag = v.unsigned_abs();
        let n = 8 - (mag.leading_zeros() / 8) as usize;
        let bytes = mag.to_be_bytes();
        let bytes = &bytes[8 - n..];
        if v > 0 {
            self.out.push(INT_ZERO + n as u8);
            self.out.extend_from_slice(bytes);
        } else {
            self.out.push(INT_ZERO - n as u8);
            self.out.extend(bytes.iter().map(|b| !b));
        }
    }

    fn big_int(&mut self, v: &BigInt) -> Result<()> {
        let (sign, mag) = v.to_bytes_be();
        let n = mag.len();
        match sign {
            Sign::NoSign => self.out.push(INT_ZERO),
            Sign::Plus if n <= 8 => {
                self.out.push(INT_ZERO + n as u8);
                self.out.extend_from_slice(&mag);
            }
            Sign::Minus if n <= 8 => {
                self.out.push(INT_ZERO - n as u8);
                self.out.extend(mag.iter().map(|b| !b));
            }
            _ if n > 255 => return Err(Error::IntegerTooLarge { bytes: n }),
            Sign::Plus => {
                self.out.push(POS_BIG_INT);
                self.out.push(n as u8);
                self.out.extend_from_slice(&mag);
            }
            Sign::Minus => {
                self.out.push(NEG_BIG_INT);
                self.out.push(!(n as u8));
                self.out.extend(mag.iter().map(|b| !b));
            }
        }
        Ok(())
    }

    fn versionstamp(&mut self, v: &Versionstamp) -> Result<()> {
        if !v.is_complete() {
            if !self.allow_incomplete {
                return Err(Error::IncompleteVersionstamp);
            }
            if self.incomplete_pos.is_some() {
                return Err(Error::MultipleIncompleteVersionstamps);
            }
            self.incomplete_pos = Some(self.out.len() + 1);
        }
        self.out.push(VERSIONSTAMP);
        self.out.extend_from_slice(&v.to_bytes());
        Ok(())
    }
}
