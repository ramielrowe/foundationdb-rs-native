//! The one accepted divergence from FDB `main`'s reader: the `work_limit` check.
//!
//! `aliased_untrusted()` is a valid `Untrusted` whose `strs` vector holds 20 offsets to
//! one 600-byte string. FDB `main` accepts it (`cpp_crosscheck` checks that); the
//! golden-vectors README classifies it as a `structural` accept with the `work_limit`
//! violation (`corpus/aliasing_bombs.jsonl`), so rejecting it is permitted, and this crate
//! does.

mod support;

use fdb_native_wire::{DecodeError, decode, encode};
use support::*;

#[test]
fn aliased_strings_hit_the_work_limit() {
    let (value, bytes) = aliased_untrusted();
    assert_eq!(bytes.len(), 824);
    assert_eq!(decode::<Untrusted>(&bytes), Err(DecodeError::WorkLimit));
    assert_eq!(DecodeError::WorkLimit.check_name(), Some("work_limit"));

    // The same value written by this crate's writer (no aliasing) decodes.
    let plain = encode(&value).unwrap();
    assert!(plain.len() > 12_000, "{}", plain.len());
    assert_eq!(decode::<Untrusted>(&plain).unwrap(), value);
}
