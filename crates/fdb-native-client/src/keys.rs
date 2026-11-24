//! Key helpers: [`strinc`].

/// `strinc(key)`: the first key after every key that starts with `key` (the bindings'
/// `strinc`, `ByteArrayUtil.strinc`): `key` without its trailing `0xff` bytes, with its last
/// byte incremented. `None` if `key` is empty or all `0xff` (no key is after all its
/// extensions).
///
/// `[key, strinc(key))` is the range of the keys that start with `key`, as the bindings'
/// `get_range_startswith` and `clear_range_startswith` read and clear it:
///
/// ```no_run
/// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
/// use fdb_native_client::strinc;
///
/// let end = strinc(b"users/").expect("not all 0xff");
/// assert_eq!(end, b"users0");
/// db.run(|tx, _| {
///     let end = end.clone();
///     async move {
///         tx.clear_range(b"users/", &end);
///         Ok(())
///     }
/// })
/// .await?;
/// # Ok(())
/// # }
/// ```
pub fn strinc(key: &[u8]) -> Option<Vec<u8>> {
    let last = key.iter().rposition(|&b| b != 0xff)?;
    let mut out = key[..=last].to_vec();
    out[last] += 1;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::strinc;

    #[test]
    fn strinc_drops_trailing_ff_and_increments() {
        assert_eq!(strinc(b"a"), Some(b"b".to_vec()));
        assert_eq!(strinc(b"users/"), Some(b"users0".to_vec()));
        assert_eq!(strinc(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(strinc(b"\x00\xfe\xff"), Some(b"\x00\xff".to_vec()));
        assert_eq!(strinc(b""), None);
        assert_eq!(strinc(b"\xff\xff"), None);
    }
}
