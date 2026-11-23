use fdb_native_wire::Wire;

// `absent = "default"` needs a `Default` impl, which the type does not have: the error
// points at the attribute.
#[derive(Wire)]
#[fdb(absent = "default")]
struct NoDefault {
    a: u8,
}

fn main() {}
