// Shape checks evaluated at compile time, each reported at the offending member.
use fdb_native_wire::Wire;

#[derive(Wire)]
#[fdb(struct)]
struct NotInline {
    a: u32,
    b: Vec<u8>,
}

#[derive(Wire, Clone, Copy)]
#[fdb(struct)]
struct Inner(u8, u32);

#[derive(Wire)]
#[fdb(struct)]
struct Nested {
    a: u8,
    inner: Inner,
}

#[derive(Wire)]
enum UnionInUnion {
    A(u8),
    B(Option<u8>),
}

fn main() {}
