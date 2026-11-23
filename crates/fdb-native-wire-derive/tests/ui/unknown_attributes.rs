use fdb_native_wire::Wire;

#[derive(Wire)]
#[fdb(ensure_table)]
struct UnknownContainer {
    a: u8,
}

#[derive(Wire)]
struct UnknownField {
    #[fdb(rename = "x")]
    a: u8,
}

#[derive(Wire)]
#[fdb(skip)]
struct FieldAttrOnType {
    a: u8,
}

#[derive(Wire)]
struct TypeAttrOnField {
    #[fdb(file_identifier = 1)]
    a: u8,
}

#[derive(Wire)]
#[fdb(file_identifier = 1, file_identifier = 2)]
struct Duplicate {
    a: u8,
}

fn main() {}
