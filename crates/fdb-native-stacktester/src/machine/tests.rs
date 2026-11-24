//! The stack machine without a cluster: the database handle points at an address where no
//! server listens, which is enough for every instruction that does not need one (the stack
//! and tuple instructions, transactions with nothing to commit, `on_error`, cancellation,
//! conflict ranges, the deferred errors and the checks made before a read). The docker test
//! (`tests/docker.rs`) runs the rest against a real cluster.

use super::*;
use crate::stack::Entry;
use crate::values::pack;

fn db() -> Database {
    // Port 1 on the loopback: nothing listens there; nothing here needs it to.
    Database::from_connection_string("stacktester:test@127.0.0.1:1").expect("connection string")
}

fn machine(db: &Database) -> StackMachine {
    StackMachine::new(Shared::new(db.clone()), b"prefix".to_vec()).unwrap()
}

fn s(op: &str) -> Element {
    Element::String(op.to_string())
}

fn b(v: &[u8]) -> Element {
    Element::Bytes(v.to_vec())
}

fn i(v: i64) -> Element {
    Element::Int(v)
}

/// An instruction: the operation and its argument, if any.
fn op(name: &str) -> Tuple {
    Tuple::from_elements(vec![s(name)])
}

fn push(item: Element) -> Tuple {
    Tuple::from_elements(vec![s("PUSH"), item])
}

/// Runs `instructions` from number 0.
async fn run(m: &mut StackMachine, instructions: &[Tuple]) -> Result<(), Fatal> {
    for (index, instruction) in instructions.iter().enumerate() {
        m.execute(index, instruction).await?;
    }
    Ok(())
}

/// The stack, bottom first, with the futures waited for.
async fn stack(m: &StackMachine) -> Vec<(usize, Element)> {
    let mut items = Vec::new();
    for item in m.stack().items() {
        items.push((item.index, item.entry.resolve().await.unwrap()));
    }
    items
}

fn error(code: u16) -> Element {
    error_value(code)
}

fn not_present() -> Element {
    b(RESULT_NOT_PRESENT)
}

#[test]
fn op_names_and_forms() {
    assert_eq!(parse_op("GET"), ("GET", Variant::Transaction));
    assert_eq!(parse_op("GET_SNAPSHOT"), ("GET", Variant::Snapshot));
    assert_eq!(
        parse_op("GET_RANGE_DATABASE"),
        ("GET_RANGE", Variant::Database)
    );
    assert_eq!(parse_op("SET_TENANT"), ("SET", Variant::Database));
    assert_eq!(parse_op("WAIT_EMPTY"), ("WAIT_EMPTY", Variant::Transaction));
}

#[tokio::test]
async fn stack_instructions() {
    let db = db();
    let mut m = machine(&db);
    run(
        &mut m,
        &[
            push(b(b"a")), // 0
            push(b(b"b")), // 1
            op("DUP"),     // 2: a b b (the copy keeps number 1)
            push(i(2)),    // 3
            op("SWAP"),    // 4: b b a
            op("POP"),     // 5: b b
            push(i(10)),   // 6
            push(i(3)),    // 7
            op("SUB"),     // 8: 3 - 10
            push(b(b"y")), // 9
            push(b(b"x")), // 10
            op("CONCAT"),  // 11: "x" + "y"
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        stack(&m).await,
        vec![(1, b(b"b")), (1, b(b"b")), (8, i(-7)), (11, b(b"xy"))]
    );
    run(&mut m, &[op("EMPTY_STACK")]).await.unwrap();
    assert!(stack(&m).await.is_empty());
}

#[tokio::test]
async fn tuple_instructions() {
    let db = db();
    let mut m = machine(&db);
    let packed = pack(vec![i(1), b(b"two"), s("three")]).unwrap();
    run(
        &mut m,
        &[
            push(s("three")),
            push(b(b"two")),
            push(i(1)),
            push(i(3)),
            op("TUPLE_PACK"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(stack(&m).await, vec![(4, b(&packed))]);

    run(&mut m, &[op("TUPLE_UNPACK")]).await.unwrap();
    assert_eq!(
        stack(&m).await,
        vec![
            (0, b(&pack(vec![i(1)]).unwrap())),
            (0, b(&pack(vec![b(b"two")]).unwrap())),
            (0, b(&pack(vec![s("three")]).unwrap())),
        ]
    );

    let mut m = machine(&db);
    run(&mut m, &[push(b(b"k")), push(i(1)), op("TUPLE_RANGE")])
        .await
        .unwrap();
    let start = pack(vec![b(b"k")]).unwrap();
    let mut begin = start.clone();
    begin.push(0x00);
    let mut end = start;
    end.push(0xff);
    assert_eq!(stack(&m).await, vec![(2, b(&begin)), (2, b(&end))]);

    let mut m = machine(&db);
    let (x, y) = (pack(vec![i(2)]).unwrap(), pack(vec![i(1)]).unwrap());
    run(
        &mut m,
        &[push(b(&x)), push(b(&y)), push(i(2)), op("TUPLE_SORT")],
    )
    .await
    .unwrap();
    // The greatest ends on top.
    assert_eq!(stack(&m).await, vec![(3, b(&y)), (3, b(&x))]);

    let mut m = machine(&db);
    run(
        &mut m,
        &[
            push(b(&1.5f32.to_be_bytes())),
            op("ENCODE_FLOAT"),
            op("DUP"),
            op("DECODE_FLOAT"),
            push(Element::Double(-2.25)),
            op("DECODE_DOUBLE"),
            op("ENCODE_DOUBLE"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        stack(&m).await,
        vec![
            (1, Element::Float(1.5)),
            (3, b(&1.5f32.to_be_bytes())),
            (6, Element::Double(-2.25)),
        ]
    );
}

#[tokio::test]
async fn tuple_pack_with_versionstamp_pushes_ok_and_the_key() {
    let db = db();
    let mut m = machine(&db);
    let stamp = Element::Versionstamp(fdb_native_tuple::Versionstamp::incomplete(3));
    run(
        &mut m,
        &[
            push(stamp.clone()),
            push(i(1)),
            push(b(b"pre")),
            op("TUPLE_PACK_WITH_VERSIONSTAMP"),
            push(i(1)),
            push(i(1)),
            push(b(b"pre")),
            op("TUPLE_PACK_WITH_VERSIONSTAMP"),
        ],
    )
    .await
    .unwrap();
    let items = stack(&m).await;
    assert_eq!(items[0], (3, b(b"OK")));
    assert_eq!(items[2], (7, b(b"ERROR: NONE")));
    let (_, Element::Bytes(key)) = &items[1] else {
        panic!("{items:?}")
    };
    assert!(key.starts_with(b"pre\x33"));
    assert_eq!(&key[key.len() - 4..], &[4, 0, 0, 0]);
}

#[tokio::test]
async fn futures_keep_their_instruction_number() {
    let db = db();
    let mut m = machine(&db);
    run(
        &mut m,
        &[
            op("NEW_TRANSACTION"), // 0
            op("COMMIT"),          // 1: nothing to commit
            push(i(7)),            // 2
            push(i(1)),            // 3
            op("SWAP"),            // 4: 7, commit
            op("WAIT_FUTURE"),     // 5
        ],
    )
    .await
    .unwrap();
    assert_eq!(stack(&m).await, vec![(2, i(7)), (1, not_present())]);
}

#[tokio::test]
async fn on_error_results() {
    let db = db();
    let mut m = machine(&db);
    run(
        &mut m,
        &[
            op("NEW_TRANSACTION"),
            push(i(1020)),
            op("ON_ERROR"), // 2: retried
            op("WAIT_FUTURE"),
            push(i(2000)),
            op("ON_ERROR"), // 5: not retryable
            op("NEW_TRANSACTION"),
            push(i(0)),
            op("ON_ERROR"), // 8: on_error(success) is a misuse
            op("NEW_TRANSACTION"),
            push(i(40_000)),
            op("ON_ERROR"), // 11: unknown_error
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        stack(&m).await,
        vec![
            (2, not_present()),
            (5, error(2000)),
            (8, error(2000)),
            (11, error(4000)),
        ]
    );
}

#[tokio::test]
async fn cancel_then_commit_pushes_transaction_cancelled() {
    let db = db();
    let mut m = machine(&db);
    run(
        &mut m,
        &[
            op("NEW_TRANSACTION"),
            op("CANCEL"),
            op("COMMIT"),
            op("WAIT_FUTURE"),
            op("RESET"),
            op("COMMIT"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(stack(&m).await, vec![(2, error(1025)), (5, not_present())]);
}

#[tokio::test]
async fn versionstamp_of_a_read_only_commit() {
    let db = db();
    let mut m = machine(&db);
    run(
        &mut m,
        &[
            op("NEW_TRANSACTION"),
            op("GET_VERSIONSTAMP"),
            op("COMMIT"),
            op("WAIT_FUTURE"),
            op("GET_COMMITTED_VERSION"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        stack(&m).await,
        vec![
            (1, error(2021)),
            (2, not_present()),
            (4, b(b"GOT_COMMITTED_VERSION")),
        ]
    );
    assert_eq!(m.last_version, -1);
}

#[tokio::test]
async fn conflict_ranges_sizes_and_deferred_errors() {
    let db = db();
    let mut m = machine(&db);
    let too_long = vec![b'k'; 20_000];
    run(
        &mut m,
        &[
            op("NEW_TRANSACTION"),
            push(b(b"b")),
            push(b(b"a")),
            op("READ_CONFLICT_RANGE"), // 3
            push(b(b"a")),
            push(b(b"b")),
            op("WRITE_CONFLICT_RANGE"), // 6: inverted
            push(b(b"k")),
            op("READ_CONFLICT_KEY"), // 8
            op("DISABLE_WRITE_CONFLICT"),
            push(b(b"v")),
            push(b(b"k")),
            op("SET"),
            op("GET_APPROXIMATE_SIZE"), // 13
            push(b(b"v")),
            push(b(&too_long)),
            op("SET"),
            op("GET_APPROXIMATE_SIZE"), // 17: the deferred key_too_large
            op("COMMIT"),               // 18
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        stack(&m).await,
        vec![
            (3, b(b"SET_CONFLICT_RANGE")),
            (6, error(2005)),
            (8, b(b"SET_CONFLICT_KEY")),
            (13, b(b"GOT_APPROXIMATE_SIZE")),
            (17, error(2102)),
            (18, error(2102)),
        ]
    );
}

#[tokio::test]
async fn range_parameters_are_checked_first() {
    let db = db();
    let mut m = machine(&db);
    let range = |mode: i64| {
        vec![
            push(i(mode)),
            push(i(0)),
            push(i(0)),
            push(b(b"z")),
            push(b(b"a")),
            op("GET_RANGE"),
        ]
    };
    let mut instructions = vec![op("NEW_TRANSACTION")];
    instructions.extend(range(0)); // exact, no limit: 2210
    instructions.extend(range(7)); // no such mode: 2000
    run(&mut m, &instructions).await.unwrap();
    assert_eq!(stack(&m).await, vec![(6, error(2210)), (12, error(2000))]);
}

#[tokio::test]
async fn transactions_are_named_and_shared() {
    let db = db();
    let shared = Shared::new(db.clone());
    let mut a = StackMachine::new(shared.clone(), b"a".to_vec()).unwrap();
    let mut other = StackMachine::new(shared, b"b".to_vec()).unwrap();
    run(
        &mut a,
        &[
            op("NEW_TRANSACTION"),
            push(b(b"v")),
            push(b(b"k")),
            op("SET"),
            push(b(b"shared")),
            op("USE_TRANSACTION"), // a new transaction named "shared"
            op("GET_APPROXIMATE_SIZE"),
        ],
    )
    .await
    .unwrap();
    // The other machine switches to `a`'s first transaction: it has the write.
    run(
        &mut other,
        &[
            push(b(b"a")),
            op("USE_TRANSACTION"),
            op("CANCEL"),
            op("GET_APPROXIMATE_SIZE"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(stack(&a).await, vec![(6, b(b"GOT_APPROXIMATE_SIZE"))]);
    assert_eq!(stack(&other).await, vec![(3, b(b"GOT_APPROXIMATE_SIZE"))]);
    // `a` cancelled through the other machine.
    run(
        &mut a,
        &[push(b(b"a")), op("USE_TRANSACTION"), op("COMMIT")],
    )
    .await
    .unwrap();
    assert_eq!(stack(&a).await[1], (2, error(1025)));
}

#[tokio::test]
async fn failures_that_are_not_fdb_errors_are_fatal() {
    let db = db();
    // A transaction used before `NEW_TRANSACTION`.
    assert!(run(&mut machine(&db), &[op("COMMIT")]).await.is_err());
    // Pops from an empty stack, wrong types, unknown operations.
    assert!(run(&mut machine(&db), &[op("POP")]).await.is_err());
    assert!(
        run(&mut machine(&db), &[push(s("x")), op("TUPLE_UNPACK")])
            .await
            .is_err()
    );
    assert!(run(&mut machine(&db), &[op("NO_SUCH_OP")]).await.is_err());
    assert!(
        run(&mut machine(&db), &[op("DIRECTORY_CREATE")])
            .await
            .is_err()
    );
    assert!(
        run(&mut machine(&db), &[op("TENANT_CREATE")])
            .await
            .is_err()
    );
    // A write has no snapshot form.
    let mut m = machine(&db);
    let snapshot_set = [
        op("NEW_TRANSACTION"),
        push(b(b"v")),
        push(b(b"k")),
        op("SET_SNAPSHOT"),
    ];
    assert!(run(&mut m, &snapshot_set).await.is_err());
    // `strinc` of a prefix of 0xff bytes.
    let mut m = machine(&db);
    let clear = [
        op("NEW_TRANSACTION"),
        push(b(b"\xff")),
        op("CLEAR_RANGE_STARTS_WITH"),
    ];
    assert!(run(&mut m, &clear).await.is_err());
    // The instruction's name must be a string.
    let bad = Tuple::from_elements(vec![b(b"PUSH"), i(1)]);
    assert!(machine(&db).execute(0, &bad).await.is_err());
}

#[tokio::test]
async fn tenant_instructions_without_tenants() {
    let db = db();
    let mut m = machine(&db);
    run(&mut m, &[op("TENANT_CLEAR_ACTIVE"), op("TENANT_GET_ID")])
        .await
        .unwrap();
    assert_eq!(stack(&m).await, vec![(1, b(b"NO_ACTIVE_TENANT"))]);
}

#[tokio::test]
async fn a_commit_has_started_when_the_next_instruction_runs() {
    // A commit that has to wait for the cluster (it has a write, and no read version) has
    // started before the next instruction runs, as `fdb_c`'s does: a read after it is
    // `used_during_commit`, which also ends the commit.
    let db = db();
    let mut m = machine(&db);
    run(
        &mut m,
        &[
            op("NEW_TRANSACTION"),
            push(b(b"v")),
            push(b(b"k")),
            op("SET"),
            op("COMMIT"),           // 4
            op("GET_READ_VERSION"), // 5
        ],
    )
    .await
    .unwrap();
    assert!(matches!(m.stack().items()[0].entry, Entry::Pending(_)));
    assert_eq!(stack(&m).await, vec![(4, error(2017)), (5, error(2017))]);
}
