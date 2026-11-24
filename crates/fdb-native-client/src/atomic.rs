//! Atomic operations: what each [`MutationType`] does to a key's value, a port of
//! `fdbclient/include/fdbclient/Atomic.h` (identical on `release-7.4` and `release-8.0` but
//! for style).
//!
//! These are the functions the storage server applies to the value a key has when an atomic
//! mutation reaches it (`convertAtomicOp`, `storageserver.actor.cpp` 7.4:7826-7881), and the
//! ones the read-your-writes layer folds a transaction's own operations with
//! (`WriteMap::coalesce`, `WriteMap.cpp` 7.4:357-494). The client uses them to work out the
//! value a transaction's own atomic operations give a key (the value a
//! [watch](crate::Transaction::watch) watches).
//!
//! # The rules, as C++ has them
//!
//! `existing` is the key's value (`None`: the key is not set); `operand` is the mutation's
//! parameter. An empty present value and a missing one are the same to most operations;
//! the `V2` operations and the byte comparisons tell them apart.
//!
//! | operation | `existing` missing | `existing` empty | `operand` empty | otherwise |
//! |---|---|---|---|---|
//! | [`add`] | `operand` | `operand` | empty | little-endian sum, `operand`'s length (carry out dropped) |
//! | [`and`] | zeros | zeros | empty | bytewise `&`, `operand`'s length, zeros past `existing` |
//! | [`and_v2`] | `operand` | as [`and`] | as [`and`] | as [`and`] |
//! | [`or`] | `operand` | `operand` | empty | bytewise `\|`, `operand`'s length, `operand`'s bytes past `existing` |
//! | [`xor`] | `operand` | `operand` | empty | bytewise `^`, as [`or`] |
//! | [`append_if_fits`] | `operand` | `operand` | `existing` | `existing ++ operand`, or `existing` if longer than the value size limit |
//! | [`max`] | `operand` | `operand` | empty | the larger as little-endian unsigned integers of `operand`'s length |
//! | [`min`] | zeros (or `operand` if all zero) | as missing | empty | the smaller, as [`max`] |
//! | [`min_v2`] | `operand` | as [`min`] | as [`min`] | as [`min`] |
//! | [`byte_max`] | `operand` | lexicographic max | lexicographic max | lexicographic max |
//! | [`byte_min`] | `operand` | lexicographic min | lexicographic min | lexicographic min |
//! | [`compare_and_clear`] | cleared | cleared if `operand` is empty | unchanged unless `existing` is empty | cleared if equal, else unchanged |
//!
//! In [`max`] and [`min`] `existing` is compared, and returned, **truncated or zero-extended
//! to `operand`'s length**: its bytes past that length are ignored. Clients of API version 510
//! and later send [`and_v2`] and [`min_v2`] in place of [`and`] and [`min`]
//! (`ReadYourWritesTransaction::atomicOp`); this client does too.
//!
//! The versionstamp helpers of `Atomic.h` are here as well: [`place_versionstamp`],
//! [`versionstamp_key_range`] (`getVersionstampKeyRange`, the range of keys a
//! `SetVersionstampedKey` may write, which the read-your-writes layer makes unreadable) and
//! [`transform_versionstamp_key`].

use fdb_native_schema::domain::{KeyRange, MutationType};

use crate::commit::VERSIONSTAMP_LEN;
use crate::error::Error;
use crate::error::codes::{CLIENT_INVALID_OPERATION, INVALID_MUTATION_TYPE, INVERTED_RANGE};

/// `VALUE_SIZE_LIMIT` (ClientKnobs 7.4 and 8.0: 100 000): the longest value, and what
/// [`append_if_fits`] does not grow a value past.
pub const VALUE_SIZE_LIMIT: usize = 100_000;

/// `existing` as C++ sees it once the optional is unwrapped (`StringRef()` if missing).
fn bytes(existing: Option<&[u8]>) -> &[u8] {
    existing.unwrap_or(&[])
}

/// `existing` truncated or zero-extended to `len` bytes (the `buf` of `doMax` / `doMin`).
fn resized(existing: &[u8], len: usize) -> Vec<u8> {
    let mut out = existing[..existing.len().min(len)].to_vec();
    out.resize(len, 0);
    out
}

/// `doLittleEndianAdd`: `existing + operand` as little-endian unsigned integers, `operand`'s
/// length (the carry out of the last byte is dropped; `existing`'s bytes past `operand`'s
/// length are ignored). A missing or empty `existing`, or an empty `operand`, gives
/// `operand`.
///
/// ```
/// use fdb_native_client::atomic::add;
///
/// assert_eq!(add(Some(&[0xff, 0x00]), &[0x01, 0x00]), vec![0x00, 0x01]);
/// assert_eq!(add(None, &5u32.to_le_bytes()), 5u32.to_le_bytes());
/// // The result has the operand's length: a 1-byte operand wraps at 256.
/// assert_eq!(add(Some(&[0xff, 0x07]), &[0x01]), vec![0x00]);
/// ```
pub fn add(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    let existing = bytes(existing);
    if existing.is_empty() || operand.is_empty() {
        return operand.to_vec();
    }
    let mut carry = 0u16;
    operand
        .iter()
        .enumerate()
        .map(|(i, &b)| {
            let sum = u16::from(b) + u16::from(existing.get(i).copied().unwrap_or(0)) + carry;
            carry = sum >> 8;
            sum as u8
        })
        .collect()
}

/// `doAnd`: bytewise `existing & operand`, `operand`'s length, with zeros where `existing`
/// is shorter (so a missing `existing` gives zeros). An empty `operand` gives it.
pub fn and(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    let existing = bytes(existing);
    operand
        .iter()
        .enumerate()
        .map(|(i, &b)| existing.get(i).map_or(0, |&e| e & b))
        .collect()
}

/// `doAndV2`: [`and`], except that a missing `existing` gives `operand`.
pub fn and_v2(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    match existing {
        None => operand.to_vec(),
        Some(_) => and(existing, operand),
    }
}

/// The shape `doOr` and `doXor` share: `f` on the bytes both have, `operand`'s bytes past
/// `existing`.
fn bytewise(existing: Option<&[u8]>, operand: &[u8], f: impl Fn(u8, u8) -> u8) -> Vec<u8> {
    let existing = bytes(existing);
    if existing.is_empty() || operand.is_empty() {
        return operand.to_vec();
    }
    operand
        .iter()
        .enumerate()
        .map(|(i, &b)| existing.get(i).map_or(b, |&e| f(e, b)))
        .collect()
}

/// `doOr`: bytewise `existing | operand`, `operand`'s length (`operand`'s bytes where
/// `existing` is shorter). A missing or empty `existing`, or an empty `operand`, gives
/// `operand`.
pub fn or(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    bytewise(existing, operand, |e, b| e | b)
}

/// `doXor`: bytewise `existing ^ operand`, as [`or`].
pub fn xor(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    bytewise(existing, operand, |e, b| e ^ b)
}

/// `doAppendIfFits`: `existing` followed by `operand`, unless that is longer than
/// `value_size_limit` (`CLIENT_KNOBS->VALUE_SIZE_LIMIT`, [`VALUE_SIZE_LIMIT`]): then
/// `existing`, unchanged. A missing or empty `existing` gives `operand`; an empty `operand`
/// gives `existing`.
pub fn append_if_fits(existing: Option<&[u8]>, operand: &[u8], value_size_limit: usize) -> Vec<u8> {
    let existing = bytes(existing);
    if existing.is_empty() {
        return operand.to_vec();
    }
    if operand.is_empty() || existing.len() + operand.len() > value_size_limit {
        return existing.to_vec();
    }
    [existing, operand].concat()
}

/// `doMax`: the larger of `existing` and `operand` as little-endian unsigned integers of
/// `operand`'s length: `operand`, or `existing` truncated or zero-extended to that length. A
/// missing or empty `existing`, or an empty `operand`, gives `operand`.
///
/// ```
/// use fdb_native_client::atomic::max;
///
/// assert_eq!(max(Some(&[0x00, 0x02]), &[0xff, 0x01]), vec![0x00, 0x02]);
/// // `existing`'s bytes past the operand's length do not count.
/// assert_eq!(max(Some(&[0x01, 0xff]), &[0x02]), vec![0x02]);
/// ```
pub fn max(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    let existing = bytes(existing);
    if existing.is_empty() || operand.is_empty() {
        return operand.to_vec();
    }
    // `operand`'s bytes past `existing`'s length: one that is not zero makes it the larger.
    if operand.len() > existing.len() && operand[existing.len()..].iter().any(|&b| b != 0) {
        return operand.to_vec();
    }
    for i in (0..operand.len().min(existing.len())).rev() {
        if operand[i] > existing[i] {
            return operand.to_vec();
        }
        if operand[i] < existing[i] {
            return resized(existing, operand.len());
        }
    }
    operand.to_vec()
}

/// `doMin`: the smaller of `existing` and `operand` as little-endian unsigned integers of
/// `operand`'s length: `operand`, or `existing` truncated or zero-extended to that length. A
/// missing `existing` counts as empty, which is zero: the result is zeros. An empty
/// `operand` gives it.
pub fn min(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    if operand.is_empty() {
        return Vec::new();
    }
    let existing = bytes(existing);
    if operand.len() > existing.len() && operand[existing.len()..].iter().any(|&b| b != 0) {
        return resized(existing, operand.len());
    }
    for i in (0..operand.len().min(existing.len())).rev() {
        if operand[i] > existing[i] {
            return resized(existing, operand.len());
        }
        if operand[i] < existing[i] {
            return operand.to_vec();
        }
    }
    operand.to_vec()
}

/// `doMinV2`: [`min`], except that a missing `existing` gives `operand`.
pub fn min_v2(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    match existing {
        None => operand.to_vec(),
        Some(_) => min(existing, operand),
    }
}

/// `doByteMax`: the lexicographically larger of `existing` and `operand` (a missing
/// `existing` gives `operand`).
pub fn byte_max(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    match existing {
        Some(e) if e > operand => e.to_vec(),
        _ => operand.to_vec(),
    }
}

/// `doByteMin`: the lexicographically smaller of `existing` and `operand` (a missing
/// `existing` gives `operand`).
pub fn byte_min(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    match existing {
        Some(e) if e < operand => e.to_vec(),
        _ => operand.to_vec(),
    }
}

/// `doCompareAndClear`: `None` (the key cleared) if `existing` is missing or equals
/// `operand`, else `existing`, unchanged.
pub fn compare_and_clear(existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
    match existing {
        Some(e) if e != operand => Some(e.to_vec()),
        _ => None,
    }
}

/// The value `op` with parameter `operand` leaves a key whose value is `existing` (`None`:
/// not set): `Ok(None)` if it clears the key. What the storage server's `convertAtomicOp`
/// makes of the mutation, and what folding the operations of a transaction in order gives
/// (`WriteMap::coalesceUnder`). `SetValue` gives `operand`. `value_size_limit` is for
/// [`append_if_fits`] ([`VALUE_SIZE_LIMIT`] by default).
///
/// `invalid_mutation_type` (2018) for `ClearRange` (a range, not a value), the versionstamp
/// operations (their result depends on the commit) and types that are not mutations.
///
/// ```
/// use fdb_native_client::MutationType;
/// use fdb_native_client::atomic::{VALUE_SIZE_LIMIT, apply};
///
/// let one = 1u64.to_le_bytes();
/// let mut value = None;
/// for _ in 0..3 {
///     value = apply(MutationType::ADD_VALUE, value.as_deref(), &one, VALUE_SIZE_LIMIT)?;
/// }
/// assert_eq!(value, Some(3u64.to_le_bytes().to_vec()));
/// # Ok::<(), fdb_native_client::Error>(())
/// ```
pub fn apply(
    op: MutationType,
    existing: Option<&[u8]>,
    operand: &[u8],
    value_size_limit: usize,
) -> Result<Option<Vec<u8>>, Error> {
    let value = match op {
        MutationType::SET_VALUE => operand.to_vec(),
        MutationType::ADD_VALUE => add(existing, operand),
        MutationType::AND => and(existing, operand),
        MutationType::AND_V2 => and_v2(existing, operand),
        MutationType::OR => or(existing, operand),
        MutationType::XOR => xor(existing, operand),
        MutationType::APPEND_IF_FITS => append_if_fits(existing, operand, value_size_limit),
        MutationType::MAX => max(existing, operand),
        MutationType::MIN => min(existing, operand),
        MutationType::MIN_V2 => min_v2(existing, operand),
        MutationType::BYTE_MAX => byte_max(existing, operand),
        MutationType::BYTE_MIN => byte_min(existing, operand),
        MutationType::COMPARE_AND_CLEAR => return Ok(compare_and_clear(existing, operand)),
        other => {
            return Err(Error::with_message(
                INVALID_MUTATION_TYPE,
                format!("mutation type {} has no value to compute", other.0),
            ));
        }
    };
    Ok(Some(value))
}

/// `placeVersionstamp`: writes the versionstamp of `version` and `transaction_number` (8
/// bytes big-endian, then 2 bytes big-endian) at the start of `destination`.
///
/// # Panics
///
/// If `destination` is shorter than 10 bytes.
pub fn place_versionstamp(destination: &mut [u8], version: i64, transaction_number: u16) {
    destination[..8].copy_from_slice(&version.to_be_bytes());
    destination[8..VERSIONSTAMP_LEN].copy_from_slice(&transaction_number.to_be_bytes());
}

/// `parseVersionstampOffset`: the little-endian `i32` of the last 4 bytes, if there are 4.
fn versionstamp_offset(data: &[u8]) -> Option<i32> {
    let split = data.len().checked_sub(4)?;
    Some(i32::from_le_bytes(
        data[split..].try_into().expect("4 bytes"),
    ))
}

/// Where 10 bytes at `offset` fit in `len` bytes (`pos < 0 || pos + 10 > len` fails).
fn placeholder_at(offset: i32, len: usize) -> Result<usize, Error> {
    match usize::try_from(offset) {
        Ok(pos)
            if pos
                .checked_add(VERSIONSTAMP_LEN)
                .is_some_and(|end| end <= len) =>
        {
            Ok(pos)
        }
        _ => Err(Error::new(CLIENT_INVALID_OPERATION)),
    }
}

/// `getVersionstampKeyRange`: the keys a `SetVersionstampedKey` of `key` (the key with its
/// placeholder and, as its last 4 bytes, the placeholder's little-endian offset) may write,
/// given the transaction's read version if it has one: from the key with the versionstamp
/// `(read_version + 1, 0)` (or zeros without a read version) in place of the placeholder, to
/// the key after the one with ten `0xff` there, but not past `max_key`. The read-your-writes
/// layer makes these keys unreadable (`accessed_unreadable`, 1036) until the commit.
///
/// `client_invalid_operation` (2000) if `key` has fewer than 4 bytes or the placeholder does
/// not fit before them; `inverted_range` (2005) if the range would start past `max_key`.
pub fn versionstamp_key_range(
    key: &[u8],
    read_version: Option<i64>,
    max_key: &[u8],
) -> Result<KeyRange, Error> {
    let offset = versionstamp_offset(key).ok_or(Error::new(CLIENT_INVALID_OPERATION))?;
    let mut begin = key[..key.len() - 4].to_vec();
    let pos = placeholder_at(offset, begin.len())?;
    let mut end = begin.clone();
    end.push(0);
    place_versionstamp(
        &mut begin[pos..],
        read_version.map_or(0, |v| v.wrapping_add(1)),
        0,
    );
    end[pos..pos + VERSIONSTAMP_LEN].fill(0xff);
    if end.as_slice() > max_key {
        end = max_key.to_vec();
    }
    KeyRange::new(begin, end).map_err(|_| Error::new(INVERTED_RANGE))
}

/// `transformVersionstampKey`: `key` with the versionstamp of `version` and
/// `transaction_number` written over the placeholder at the offset its last 4 bytes give
/// (the 4 bytes stay). `client_invalid_operation` (2000) if `key` has fewer than 4 bytes or
/// the 10 bytes do not fit in `key` (C++ checks against the whole key, offset included).
pub fn transform_versionstamp_key(
    key: &[u8],
    version: i64,
    transaction_number: u16,
) -> Result<Vec<u8>, Error> {
    let offset = versionstamp_offset(key).ok_or(Error::new(CLIENT_INVALID_OPERATION))?;
    let pos = placeholder_at(offset, key.len())?;
    let mut out = key.to_vec();
    place_versionstamp(&mut out[pos..], version, transaction_number);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = VALUE_SIZE_LIMIT;

    /// `(existing, operand, expected)` cases for a binary operation.
    type Case<'a> = (Option<&'a [u8]>, &'a [u8], &'a [u8]);

    fn check(name: &str, f: impl Fn(Option<&[u8]>, &[u8]) -> Vec<u8>, cases: &[Case<'_>]) {
        for (i, (existing, operand, expected)) in cases.iter().enumerate() {
            assert_eq!(
                f(*existing, operand),
                expected.to_vec(),
                "{name} case {i}: {existing:?} {operand:?}"
            );
        }
    }

    #[test]
    fn little_endian_add() {
        check(
            "add",
            add,
            &[
                // A missing or empty value, or an empty operand: the operand.
                (None, &[1, 2], &[1, 2]),
                (Some(&[]), &[1, 2], &[1, 2]),
                (Some(&[9, 9]), &[], &[]),
                (None, &[], &[]),
                // The sum, with carries.
                (Some(&[1, 0]), &[2, 0], &[3, 0]),
                (Some(&[0xff, 0x00]), &[0x01, 0x00], &[0x00, 0x01]),
                (Some(&[0xff, 0xff]), &[0x01, 0x00], &[0x00, 0x00]), // overflow: carry dropped
                (
                    Some(&[0xff, 0xff, 0xff]),
                    &[0xff, 0xff, 0xff],
                    &[0xfe, 0xff, 0xff],
                ),
                // The operand's length: a longer value is truncated...
                (Some(&[0xff, 0x07]), &[0x01], &[0x00]),
                (Some(&[0x01, 0x02, 0x03]), &[0x01, 0x01], &[0x02, 0x03]),
                // ... a shorter one extended with zeros (the carry goes on).
                (Some(&[0xff]), &[0x01, 0x00, 0x00], &[0x00, 0x01, 0x00]),
                (Some(&[0xff]), &[0xff, 0xff, 0x00], &[0xfe, 0x00, 0x01]),
            ],
        );
        // Counters: 64-bit increments and a negative (two's complement) one.
        let n = add(Some(&41u64.to_le_bytes()), &1u64.to_le_bytes());
        assert_eq!(n, 42u64.to_le_bytes());
        let n = add(Some(&42i64.to_le_bytes()), &(-50i64).to_le_bytes());
        assert_eq!(n, (-8i64).to_le_bytes());
        let n = add(Some(&u64::MAX.to_le_bytes()), &1u64.to_le_bytes());
        assert_eq!(n, 0u64.to_le_bytes());
    }

    #[test]
    fn bit_and() {
        let cases: &[Case<'_>] = &[
            (Some(&[0b1100, 0xff]), &[0b1010, 0x0f], &[0b1000, 0x0f]),
            // Shorter value: zeros past it. Longer: truncated.
            (Some(&[0xff]), &[0x0f, 0xff, 0x01], &[0x0f, 0, 0]),
            (Some(&[0xff, 0xff, 0xff]), &[0xf0], &[0xf0]),
            // An empty value is zero.
            (Some(&[]), &[0xff, 0xff], &[0, 0]),
            // An empty operand: empty.
            (Some(&[0xff]), &[], &[]),
        ];
        check("and", and, cases);
        check("and_v2", and_v2, cases);
        // The difference: a missing value is zeros for `And`, the operand for `AndV2`.
        check(
            "and",
            and,
            &[(None, &[0xff, 0x01], &[0, 0]), (None, &[], &[])],
        );
        check(
            "and_v2",
            and_v2,
            &[(None, &[0xff, 0x01], &[0xff, 0x01]), (None, &[], &[])],
        );
    }

    #[test]
    fn bit_or_and_xor() {
        check(
            "or",
            or,
            &[
                (None, &[1, 2], &[1, 2]),
                (Some(&[]), &[1, 2], &[1, 2]),
                (Some(&[0xf0]), &[], &[]),
                (Some(&[0b1100, 0x00]), &[0b1010, 0x01], &[0b1110, 0x01]),
                // The operand's bytes past a shorter value; a longer value truncated.
                (Some(&[0x01]), &[0x10, 0x22, 0x33], &[0x11, 0x22, 0x33]),
                (Some(&[0x01, 0xff, 0xff]), &[0x10], &[0x11]),
            ],
        );
        check(
            "xor",
            xor,
            &[
                (None, &[1, 2], &[1, 2]),
                (Some(&[]), &[1, 2], &[1, 2]),
                (Some(&[0xf0]), &[], &[]),
                (Some(&[0b1100, 0xff]), &[0b1010, 0xff], &[0b0110, 0x00]),
                (Some(&[0xff]), &[0x0f, 0x22, 0x33], &[0xf0, 0x22, 0x33]),
                (Some(&[0x01, 0xff, 0xff]), &[0x01], &[0x00]),
            ],
        );
    }

    #[test]
    fn append_if_it_fits() {
        let f = |e: Option<&[u8]>, o: &[u8]| append_if_fits(e, o, LIMIT);
        check(
            "append_if_fits",
            f,
            &[
                (None, b"ab", b"ab"),
                (Some(b""), b"ab", b"ab"),
                (Some(b"ab"), b"", b"ab"), // an empty operand: the value
                (None, b"", b""),
                (Some(b"ab"), b"cd", b"abcd"),
            ],
        );
        // At the limit it fits; one byte more and the value stays as it is.
        let existing = vec![1u8; LIMIT - 2];
        assert_eq!(append_if_fits(Some(&existing), &[2, 2], LIMIT).len(), LIMIT);
        assert_eq!(append_if_fits(Some(&existing), &[2, 2, 2], LIMIT), existing);
        // The limit is a parameter (the knob).
        assert_eq!(append_if_fits(Some(b"abc"), b"de", 4), b"abc");
        assert_eq!(append_if_fits(Some(b"abc"), b"d", 4), b"abcd");
        // An operand alone longer than the limit is not checked (C++ does not).
        assert_eq!(append_if_fits(None, b"abcdef", 4), b"abcdef");
    }

    #[test]
    fn max_as_little_endian() {
        check(
            "max",
            max,
            &[
                (None, &[1, 2], &[1, 2]),
                (Some(&[]), &[1, 2], &[1, 2]),
                (Some(&[5]), &[], &[]),
                // Same length: compared from the most significant (last) byte.
                (Some(&[0x00, 0x02]), &[0xff, 0x01], &[0x00, 0x02]),
                (Some(&[0xff, 0x01]), &[0x00, 0x02], &[0x00, 0x02]),
                (Some(&[0x07, 0x01]), &[0x07, 0x01], &[0x07, 0x01]), // equal: the operand
                // A longer operand with a high byte set wins at once.
                (Some(&[0xff]), &[0x00, 0x01], &[0x00, 0x01]),
                // ... with zero high bytes, the low bytes decide; the value is zero-extended.
                (Some(&[0xff]), &[0x01, 0x00, 0x00], &[0xff, 0x00, 0x00]),
                (Some(&[0x01]), &[0xff, 0x00], &[0xff, 0x00]),
                // A longer value: its bytes past the operand's length are ignored, and the
                // result is truncated.
                (Some(&[0x01, 0xff]), &[0x02], &[0x02]),
                (Some(&[0x03, 0xff]), &[0x02], &[0x03]),
            ],
        );
        assert_eq!(
            max(Some(&300u32.to_le_bytes()), &299u32.to_le_bytes()),
            300u32.to_le_bytes()
        );
    }

    #[test]
    fn min_as_little_endian() {
        let cases: &[Case<'_>] = &[
            (Some(&[5]), &[], &[]),
            (Some(&[0x00, 0x02]), &[0xff, 0x01], &[0xff, 0x01]),
            (Some(&[0xff, 0x01]), &[0x00, 0x02], &[0xff, 0x01]),
            (Some(&[0x07, 0x01]), &[0x07, 0x01], &[0x07, 0x01]),
            // A longer operand with a high byte set: the value, zero-extended.
            (Some(&[0xff]), &[0x00, 0x01], &[0xff, 0x00]),
            // ... zero high bytes: the low bytes decide.
            (Some(&[0xff]), &[0x01, 0x00, 0x00], &[0x01, 0x00, 0x00]),
            (Some(&[0x01]), &[0xff, 0x00], &[0x01, 0x00]),
            // A longer value: truncated.
            (Some(&[0x01, 0xff]), &[0x02], &[0x01]),
            (Some(&[0x03, 0x00]), &[0x02], &[0x02]),
            // An empty value is zero.
            (Some(&[]), &[0x05, 0x00], &[0x00, 0x00]),
            (Some(&[]), &[0x00, 0x00], &[0x00, 0x00]),
        ];
        check("min", min, cases);
        check("min_v2", min_v2, cases);
        // The difference: a missing value is zero for `Min`, the operand for `MinV2`.
        check(
            "min",
            min,
            &[(None, &[0x05, 0x01], &[0, 0]), (None, &[0, 0], &[0, 0])],
        );
        check("min_v2", min_v2, &[(None, &[0x05, 0x01], &[0x05, 0x01])]);
    }

    #[test]
    fn byte_comparisons() {
        check(
            "byte_max",
            byte_max,
            &[
                (None, b"b", b"b"),
                (Some(b""), b"b", b"b"),
                (Some(b"b"), b"", b"b"),
                (Some(b"b"), b"a", b"b"),
                (Some(b"a"), b"b", b"b"),
                (Some(b"ab"), b"a", b"ab"), // a prefix is smaller
                (Some(b"a"), b"ab", b"ab"),
                (Some(b"\xff"), b"\x00\xff", b"\xff"), // not as integers
            ],
        );
        check(
            "byte_min",
            byte_min,
            &[
                (None, b"b", b"b"),
                (Some(b""), b"b", b""),
                (Some(b"b"), b"", b""),
                (Some(b"b"), b"a", b"a"),
                (Some(b"a"), b"b", b"a"),
                (Some(b"ab"), b"a", b"a"),
                (Some(b"a"), b"ab", b"a"),
                (Some(b"\xff"), b"\x00\xff", b"\x00\xff"),
            ],
        );
    }

    #[test]
    fn compare_and_clear_clears_on_equal_or_missing() {
        assert_eq!(compare_and_clear(None, b"x"), None);
        assert_eq!(compare_and_clear(Some(b"x"), b"x"), None);
        assert_eq!(compare_and_clear(Some(b"x"), b"y"), Some(b"x".to_vec()));
        assert_eq!(compare_and_clear(Some(b""), b""), None);
        assert_eq!(compare_and_clear(Some(b""), b"y"), Some(Vec::new()));
        assert_eq!(compare_and_clear(Some(b"x"), b""), Some(b"x".to_vec()));
    }

    #[test]
    fn apply_dispatches_every_atomic_operation() {
        let e: Option<&[u8]> = Some(&[0x0f, 0x01]);
        let o: &[u8] = &[0xf1, 0x00];
        let cases = [
            (MutationType::SET_VALUE, Some(o.to_vec())),
            (MutationType::ADD_VALUE, Some(add(e, o))),
            (MutationType::AND, Some(and(e, o))),
            (MutationType::AND_V2, Some(and_v2(e, o))),
            (MutationType::OR, Some(or(e, o))),
            (MutationType::XOR, Some(xor(e, o))),
            (
                MutationType::APPEND_IF_FITS,
                Some(append_if_fits(e, o, LIMIT)),
            ),
            (MutationType::MAX, Some(max(e, o))),
            (MutationType::MIN, Some(min(e, o))),
            (MutationType::MIN_V2, Some(min_v2(e, o))),
            (MutationType::BYTE_MAX, Some(byte_max(e, o))),
            (MutationType::BYTE_MIN, Some(byte_min(e, o))),
            (MutationType::COMPARE_AND_CLEAR, compare_and_clear(e, o)),
        ];
        for (op, expected) in cases {
            assert_eq!(apply(op, e, o, LIMIT).unwrap(), expected, "{op:?}");
        }
        assert_eq!(
            apply(MutationType::COMPARE_AND_CLEAR, e, &[0x0f, 0x01], LIMIT).unwrap(),
            None
        );
        for op in [
            MutationType::CLEAR_RANGE,
            MutationType::SET_VERSIONSTAMPED_KEY,
            MutationType::SET_VERSIONSTAMPED_VALUE,
            MutationType(3),
            MutationType(200),
        ] {
            assert_eq!(
                apply(op, e, o, LIMIT).unwrap_err().code(),
                INVALID_MUTATION_TYPE,
                "{op:?}"
            );
        }
        // Applied in order, as the storage server applies a transaction's mutations.
        let mut v = Some(b"seed".to_vec());
        for (op, operand) in [
            (MutationType::SET_VALUE, &b"\x01\x00"[..]),
            (MutationType::ADD_VALUE, b"\xff\x00"),
            (MutationType::MAX, b"\x00\x02"),
            (MutationType::COMPARE_AND_CLEAR, b"\x00\x02"),
            (MutationType::MIN_V2, b"\x07"),
            (MutationType::APPEND_IF_FITS, b"z"),
        ] {
            v = apply(op, v.as_deref(), operand, LIMIT).unwrap();
        }
        assert_eq!(v.as_deref(), Some(&b"\x07z"[..]));
    }

    #[test]
    fn versionstamps() {
        let mut buf = [0u8; 12];
        place_versionstamp(&mut buf, 0x0102_0304_0506_0708, 0x090a);
        assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0]);

        // `k` + 10-byte placeholder + `/z`, offset 1.
        let mut key = b"k".to_vec();
        key.extend_from_slice(&[0xaa; 10]);
        key.extend_from_slice(b"/z");
        key.extend_from_slice(&1i32.to_le_bytes());

        let range = versionstamp_key_range(&key, Some(41), b"\xff").unwrap();
        let mut begin = b"k".to_vec();
        begin.extend_from_slice(&42i64.to_be_bytes());
        begin.extend_from_slice(&[0, 0]);
        begin.extend_from_slice(b"/z");
        let mut end = b"k".to_vec();
        end.extend_from_slice(&[0xff; 10]);
        end.extend_from_slice(b"/z\x00");
        assert_eq!(range, KeyRange { begin, end });
        // Without a read version the stamp is zeros.
        let range = versionstamp_key_range(&key, None, b"\xff").unwrap();
        assert_eq!(&range.begin[1..11], &[0; 10]);
        // Clamped to the max key.
        let range = versionstamp_key_range(&key, None, b"k\x80").unwrap();
        assert_eq!(range.end, b"k\x80");
        // Starting past the max key: inverted.
        assert_eq!(
            versionstamp_key_range(&key, None, b"a").unwrap_err().code(),
            INVERTED_RANGE
        );
        // Bad offsets.
        for bad in [
            &b"abc"[..],
            &[0u8; 13][..], // offset 0, but 10 bytes do not fit in the 9 before it
            &[0xff, 0xff, 0xff, 0xff][..],
        ] {
            assert_eq!(
                versionstamp_key_range(bad, None, b"\xff")
                    .unwrap_err()
                    .code(),
                CLIENT_INVALID_OPERATION
            );
        }

        // The transformed key keeps its 4 bytes of offset.
        let t = transform_versionstamp_key(&key, 42, 7).unwrap();
        assert_eq!(t.len(), key.len());
        assert_eq!(&t[1..9], &42i64.to_be_bytes());
        assert_eq!(&t[9..11], &[0, 7]);
        assert_eq!(&t[11..], &key[11..]);
        // C++ checks the placeholder against the whole key here.
        let mut short = vec![0u8; 10];
        short.extend_from_slice(&0i32.to_le_bytes());
        assert!(transform_versionstamp_key(&short, 1, 0).is_ok());
        assert!(versionstamp_key_range(&short, None, b"\xff").is_ok());
        let mut over = vec![0u8; 9];
        over.extend_from_slice(&1i32.to_le_bytes());
        assert!(transform_versionstamp_key(&over, 1, 0).is_ok());
        assert_eq!(
            versionstamp_key_range(&over, None, b"\xff")
                .unwrap_err()
                .code(),
            CLIENT_INVALID_OPERATION
        );
        assert!(transform_versionstamp_key(b"abc", 1, 0).is_err());
    }
}
