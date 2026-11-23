use fdb_native_wire::Wire;

// A union's absent value is its first alternative's: the attribute goes there.
#[derive(Wire)]
#[fdb(absent = "default")]
enum Union {
    A(u8),
    B(u16),
}

// Only "default" is a value.
#[derive(Wire)]
#[fdb(absent = "zero")]
struct UnknownValue {
    a: u8,
}

// A string, not an expression.
#[derive(Wire)]
#[fdb(absent = default)]
struct NotAString {
    a: u8,
}

// A container attribute.
#[derive(Wire)]
struct OnField {
    #[fdb(absent = "default")]
    a: u8,
}

#[derive(Wire)]
#[fdb(absent = "default", absent = "default")]
struct Duplicate {
    a: u8,
}

fn main() {}
