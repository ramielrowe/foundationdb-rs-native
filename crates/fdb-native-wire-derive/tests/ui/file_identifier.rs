use fdb_native_wire::Wire;

// A literal is checked by the derive.
#[derive(Wire)]
#[fdb(file_identifier = 0x01000000)]
struct TooLarge {
    a: u8,
}

#[derive(Wire)]
#[fdb(file_identifier)]
struct Missing {
    a: u8,
}

fn main() {}
