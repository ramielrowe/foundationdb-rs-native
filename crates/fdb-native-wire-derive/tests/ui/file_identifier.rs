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

#[derive(Wire)]
#[fdb(file_identifier = if true { 1 } else { 2 })]
struct NotAConstant {
    a: u8,
}

// std::variant and std::tuple have no file identifier.
#[derive(Wire)]
#[fdb(file_identifier = 1)]
enum Union {
    A(u8),
}

#[derive(Wire)]
#[fdb(struct, file_identifier = 1)]
struct Inline {
    a: u8,
}

fn main() {}
