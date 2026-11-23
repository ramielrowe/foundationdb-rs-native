use fdb_native_wire::Wire;

#[derive(Wire)]
struct SkipAndArena {
    a: u8,
    #[fdb(skip, arena)]
    b: (),
}

#[derive(Wire)]
struct ZeroSlots {
    a: u8,
    #[fdb(skip_slots = 0)]
    b: (),
}

#[derive(Wire)]
struct ArenaNotLast {
    #[fdb(arena)]
    arena: (),
    a: u8,
}

#[derive(Wire)]
struct TwoArenas {
    a: u8,
    #[fdb(arena)]
    one: (),
    #[fdb(arena)]
    two: (),
}

#[derive(Wire)]
#[fdb(struct)]
struct ArenaInStruct {
    a: u8,
    #[fdb(arena)]
    arena: (),
}

fn main() {}
