use fdb_native_wire::Wire;

#[derive(Wire)]
enum BadVariants {
    Unit,
    Two(u8, u16),
    Named { a: u8 },
    #[fdb(skip)]
    Attr(u8),
}

#[derive(Wire)]
enum Empty {}

#[derive(Wire)]
#[fdb(struct)]
enum StructEnum {
    A(u8),
}

#[derive(Wire)]
union RustUnion {
    a: u8,
}

fn main() {}
