//! Docker test harness for running tests against disposable FoundationDB containers.
//!
//! Tests that need a live cluster must only ever target containers started by this crate.

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
