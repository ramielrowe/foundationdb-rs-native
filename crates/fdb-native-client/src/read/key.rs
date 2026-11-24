//! `getKey`: resolving a key selector, shard by shard. See the [module
//! documentation](super#key-selectors) for how it follows C++.

use std::sync::Arc;

use fdb_native_schema::domain::{GetKey, KeySelector};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::VersionVector;

use super::location::{KEY_SPACE_END, Location};
use super::{ReadParams, ReadSnapshot, Reader, Shared, lock};
use crate::error::Error;
use crate::error::codes::*;

/// `KeySelectorRef::isBackward` (`FDBTypes.h` 7.4:656-658): whether the selector resolves
/// from the keys before its key only (`!orEqual && offset <= 0`).
pub(crate) fn is_backward(selector: &KeySelector) -> bool {
    !selector.or_equal && selector.offset <= 0
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Reader<R, W> {
    /// The key `selector` resolves to at `version` (`getKey`), across as many shards as it
    /// takes.
    ///
    /// The result is clamped to the key space: a selector that resolves before the first key
    /// gives the empty key (`allKeys.begin`), and one that resolves past the last key (the
    /// system keys `\xff...` included) gives [`KEY_SPACE_END`] (`\xff\xff`, `allKeys.end`).
    /// The transaction's `get_key` clamps that further to the keys it may read.
    ///
    /// `version` must be a read version (positive), else `client_invalid_operation`; a
    /// selector key after [`KEY_SPACE_END`] is `key_outside_legal_range`. Unlike a
    /// transaction, the reader does not shorten a selector key longer than any key can be
    /// (`KeySelectorRef::setKey`): it is sent as it is.
    pub async fn get_key(&self, selector: &KeySelector, version: i64) -> Result<Vec<u8>, Error> {
        self.get_key_with(selector, version, false).await
    }

    /// [`get_key`](Self::get_key) with `params` (as [`Reader::get_with`]): keys are located
    /// with provisional commit proxies too if
    /// [`use_provisional_proxies`](ReadParams::use_provisional_proxies), and every
    /// `GetKeyRequest` carries [`read_options`](ReadParams::read_options).
    pub async fn get_key_with(
        &self,
        selector: &KeySelector,
        version: i64,
        params: impl Into<ReadParams>,
    ) -> Result<Vec<u8>, Error> {
        if version <= 0 {
            return Err(Error::new(CLIENT_INVALID_OPERATION));
        }
        if selector.key.as_slice() > KEY_SPACE_END {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        get_key(&self.inner.shared, selector, version, params.into()).await
    }
}

/// `getKey`'s loop, after [`Reader::get_key_with`]'s argument checks (`getRangeFallback`
/// resolves its selectors with it too).
pub(crate) async fn get_key<R: Runtime, W: StreamWrapper<R::Stream>>(
    shared: &Shared<R, W>,
    selector: &KeySelector,
    version: i64,
    params: ReadParams,
) -> Result<Vec<u8>, Error> {
    {
        let mut k = selector.clone();
        // As in `get_with`: the previous attempt's location stays alive until the next one is
        // known.
        let mut _previous: Option<Arc<Location>> = None;
        loop {
            if k.key.as_slice() == KEY_SPACE_END {
                if k.offset > 0 {
                    return Ok(KEY_SPACE_END.to_vec());
                }
                k.or_equal = false;
            } else if k.key.is_empty() && k.offset <= 0 {
                return Ok(Vec::new());
            } else if k.key.as_slice() > KEY_SPACE_END {
                // Only a storage server that does not follow `findKey` could send this
                // (C++ asserts in `getKeyLocation_internal`).
                return Err(Error::with_message(
                    INTERNAL_ERROR,
                    "a storage server resolved a key selector past the end of the key space",
                ));
            }
            let backward = is_backward(&k);
            let (_, location) = shared
                .key_location_for(
                    &k.key,
                    version,
                    params.use_provisional_proxies,
                    backward,
                    STORAGE_SERVER_STREAMS.get_key,
                )
                .await?;
            _previous = Some(location.clone());
            let request = GetKey {
                sel: k.clone(),
                version,
                tags: None,
                span_context: Default::default(),
                options: params.read_options,
                ss_latest_commit_versions: VersionVector::default(),
            };
            match shared.load_balance(&location, request).await {
                Ok(sel) => {
                    // Resolved (`KeySelectorRef(key, true, 0)`), or the rest of the way from
                    // the end of what the server could see: its shard boundary, or where a
                    // byte-limited scan stopped.
                    k = sel;
                    if k.offset == 0 && k.or_equal {
                        return Ok(k.key);
                    }
                }
                Err(e) if e.code() == WRONG_SHARD_SERVER || e.code() == ALL_ALTERNATIVES_FAILED => {
                    if backward {
                        lock(&shared.locations).invalidate_key_before(&k.key);
                    } else {
                        lock(&shared.locations).invalidate_key(&k.key);
                    }
                    shared
                        .rt
                        .sleep(shared.config.wrong_shard_server_delay)
                        .await;
                }
                Err(e) => return Err(e),
            }
            shared.check_open()?;
        }
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> ReadSnapshot<R, W> {
    /// The key `selector` resolves to at the snapshot's version ([`Reader::get_key_with`]).
    pub async fn get_key(&self, selector: &KeySelector) -> Result<Vec<u8>, Error> {
        self.reader
            .get_key_with(selector, self.version, self.params)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backward_selectors() {
        assert!(is_backward(&KeySelector::last_less_than(b"a".to_vec())));
        assert!(!is_backward(&KeySelector::last_less_or_equal(
            b"a".to_vec()
        )));
        assert!(!is_backward(&KeySelector::first_greater_than(
            b"a".to_vec()
        )));
        assert!(!is_backward(&KeySelector::first_greater_or_equal(
            b"a".to_vec()
        )));
        assert!(is_backward(&KeySelector::new(b"a".to_vec(), false, -3)));
        assert!(!is_backward(&KeySelector::new(b"a".to_vec(), true, -3)));
    }
}
