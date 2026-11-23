//! The errors of the commit path, as [`Error`]s with their FDB codes.
//!
//! | code | when |
//! | --- | --- |
//! | 2102 `key_too_large` | a set/atomic key over the limit |
//! | 2103 `value_too_large` | a value over 100000 bytes |
//! | 2101 `transaction_too_large` | `getSize()` over the size limit |
//! | 2023 `transaction_read_only` | read-only option and writes |
//! | 2006 `invalid_option_value` | the size limit option out of range |
//! | 2005 `inverted_range` | a range with `begin > end` |
//! | 2000 `client_invalid_operation` | a mutation type this client does not send |
//! | 1020 `not_committed` | the transaction conflicted ([`Error::conflicting_ranges`]) |
//! | 1021 `commit_unknown_result` | the commit may have happened |
//! | the proxy's, or the transport's | any other error the proxy replied, or a transport failure ([`Error::from_commit_request`]) |
//! | 1101 `operation_cancelled` | the cluster monitor was shut down before the commit was sent |
//! | 4100 `internal_error` | a request that cannot be built |

use crate::error::Error;
use crate::error::codes::*;

pub(crate) fn key_too_large(size: usize, limit: usize) -> Error {
    Error::with_message(
        KEY_TOO_LARGE,
        format!("a key of {size} bytes exceeds the limit of {limit}"),
    )
}

pub(crate) fn value_too_large(size: usize, limit: usize) -> Error {
    Error::with_message(
        VALUE_TOO_LARGE,
        format!("a value of {size} bytes exceeds the limit of {limit}"),
    )
}

pub(crate) fn transaction_too_large(size: u64, limit: u64) -> Error {
    Error::with_message(
        TRANSACTION_TOO_LARGE,
        format!("{size} bytes exceed the limit of {limit}"),
    )
}

pub(crate) fn invalid_option(message: &'static str) -> Error {
    Error::with_message(INVALID_OPTION_VALUE, message)
}

pub(crate) fn unsupported(mutation_type: u8) -> Error {
    Error::with_message(
        CLIENT_INVALID_OPERATION,
        format!("mutation type {mutation_type} is not supported"),
    )
}

pub(crate) fn closed() -> Error {
    Error::with_message(OPERATION_CANCELLED, "the database was shut down")
}

pub(crate) fn internal(message: String) -> Error {
    Error::with_message(INTERNAL_ERROR, message)
}

pub(crate) fn inverted_range() -> Error {
    Error::with_message(INVERTED_RANGE, "begin is larger than end")
}
