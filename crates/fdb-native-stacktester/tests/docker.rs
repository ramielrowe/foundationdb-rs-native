//! The stack tester binary against a real, disposable FDB server started by
//! `fdb-native-testkit` (image from `FDB_IMAGE`, default 7.4.10): hand-written instruction
//! sequences are written under a prefix as the bindingtester writes them
//! (`pack((prefix, n)) = pack(instruction)`), the binary runs them, and the test reads what
//! `LOG_STACK` logged and what the instructions wrote. Run with
//! `cargo nextest run -p fdb-native-stacktester --run-ignored only -E 'binary(docker)'`.

use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

use fdb_native_client::{Database, Error, KeySelector, RangeOptions, StreamingMode, strinc};
use fdb_native_testkit::FdbContainer;
use fdb_native_tuple::{Element, Tuple};

const LIMIT: Duration = Duration::from_secs(300);

fn b(v: &[u8]) -> Element {
    Element::Bytes(v.to_vec())
}

fn pack(items: Vec<Element>) -> Vec<u8> {
    Tuple::from_elements(items).pack().unwrap()
}

fn error(code: u16) -> Element {
    b(&pack(vec![b(b"ERROR"), b(code.to_string().as_bytes())]))
}

const NOT_PRESENT: &[u8] = b"RESULT_NOT_PRESENT";

/// A list of instructions, numbered as they are added.
#[derive(Default)]
struct Script {
    instructions: Vec<Tuple>,
}

impl Script {
    fn op(&mut self, name: &str) -> usize {
        self.instructions
            .push(Tuple::from_elements(vec![Element::String(name.into())]));
        self.instructions.len() - 1
    }

    fn push(&mut self, item: Element) -> usize {
        self.instructions.push(Tuple::from_elements(vec![
            Element::String("PUSH".into()),
            item,
        ]));
        self.instructions.len() - 1
    }

    fn bytes(&mut self, v: &[u8]) -> usize {
        self.push(b(v))
    }

    fn int(&mut self, v: i64) -> usize {
        self.push(Element::Int(v))
    }

    fn string(&mut self, v: &str) -> usize {
        self.push(Element::String(v.into()))
    }

    /// Writes the instructions under `prefix`.
    async fn store(&self, db: &Database, prefix: &[u8]) -> Result<(), Error> {
        let rows: Vec<(Vec<u8>, Vec<u8>)> = self
            .instructions
            .iter()
            .enumerate()
            .map(|(i, instruction)| {
                (
                    pack(vec![b(prefix), Element::Int(i as i64)]),
                    instruction.pack().unwrap(),
                )
            })
            .collect();
        db.run(|tx, _| {
            for (key, value) in &rows {
                tx.set(key, value);
            }
            std::future::ready(Ok(()))
        })
        .await
    }
}

/// Runs the tester for `prefix`.
async fn run_tester(prefix: &str, cluster_file: &Path) -> Output {
    let cluster_file = cluster_file.to_path_buf();
    let prefix = prefix.to_string();
    let run = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_fdb-native-stacktester"))
            .arg(prefix)
            .arg("740")
            .arg(cluster_file)
            .output()
            .expect("the tester runs")
    });
    tokio::time::timeout(LIMIT, run)
        .await
        .expect("the tester finishes in time")
        .unwrap()
}

/// The rows under `prefix`.
async fn read_prefix(db: &Database, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let end = strinc(prefix).unwrap();
    let options = RangeOptions::default().mode(StreamingMode::WantAll);
    db.run(|tx, _| {
        let read = tx.get_range(
            &KeySelector::first_greater_or_equal(prefix.to_vec()),
            &KeySelector::first_greater_or_equal(end.clone()),
            &options,
        );
        async move { Ok(read.await?.kvs) }
    })
    .await
    .unwrap()
    .into_iter()
    .map(|kv| (kv.key, kv.value))
    .collect()
}

/// What `LOG_STACK` wrote under `prefix`: `(instruction number, item)`, bottom first.
async fn logged_stack(db: &Database, prefix: &[u8]) -> Vec<(usize, Element)> {
    let mut items = Vec::new();
    for (i, (key, value)) in read_prefix(db, prefix).await.into_iter().enumerate() {
        let key = Tuple::unpack(&key[prefix.len()..]).unwrap();
        assert_eq!(key[0], Element::Int(i as i64), "stack index");
        let Element::Int(number) = key[1] else {
            panic!("{key:?}")
        };
        let value = Tuple::unpack(&value).unwrap();
        items.push((number as usize, value[0].clone()));
    }
    items
}

/// An expected item: a value, or the 10-byte stamp of `GET_VERSIONSTAMP`.
#[derive(Debug)]
enum Expected {
    Value(Element),
    Stamp,
}

async fn start() -> (FdbContainer, Database) {
    let fdb = tokio::task::spawn_blocking(FdbContainer::start)
        .await
        .unwrap()
        .expect("container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    let db = Database::open(Some(fdb.cluster_file())).expect("database");
    (fdb, db)
}

async fn stop(fdb: FdbContainer, db: Database) {
    db.shutdown().await;
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}

fn assert_success(out: &Output) {
    assert!(
        out.status.success(),
        "the tester failed ({}): {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_stacktester_runs_a_script() {
    let (fdb, db) = start().await;
    let prefix = b"stk_main";
    let thread_prefix = b"stk_thread";
    let results = b"stk/results/".to_vec();
    let ws = b"stk/ws/".to_vec();
    let ws_end = strinc(&ws).unwrap();
    let key = |name: &str| [ws.as_slice(), name.as_bytes()].concat();
    let (foo1, foo2, foo3, foo4) = (key("foo1"), key("foo2"), key("foo3"), key("foo4"));
    let wait_prefix = key("wait/");
    let wait_key = key("wait/thread");
    let thread_key = key("thread");
    let stamp_prefix = key("stamp/");

    let mut s = Script::default();
    let mut expected: Vec<(usize, Expected)> = Vec::new();
    let mut expect = |i: usize, v: Element| expected.push((i, Expected::Value(v)));

    s.op("NEW_TRANSACTION");
    s.bytes(b"bar");
    s.bytes(&foo1);
    s.op("SET");
    s.bytes(&foo1);
    expect(s.op("GET"), b(b"bar")); // read-your-writes
    expect(s.op("COMMIT"), b(NOT_PRESENT));
    s.op("WAIT_FUTURE");
    s.op("RESET");
    expect(s.op("GET_READ_VERSION"), b(b"GOT_READ_VERSION"));
    s.bytes(&foo1);
    expect(s.op("GET_SNAPSHOT"), b(b"bar"));
    s.bytes(&foo1);
    expect(s.op("GET_DATABASE"), b(b"bar"));
    s.bytes(&foo2);
    expect(s.op("GET"), b(NOT_PRESENT));
    s.bytes(b"bar2");
    s.bytes(&foo2);
    expect(s.op("SET_DATABASE"), b(NOT_PRESENT));

    // GET_RANGE_DATABASE: BEGIN, END, LIMIT, REVERSE, STREAMING_MODE (iterator).
    s.int(-1);
    s.int(0);
    s.int(0);
    s.bytes(&ws_end);
    s.bytes(&ws);
    let all = pack(vec![b(&foo1), b(b"bar"), b(&foo2), b(b"bar2")]);
    expect(s.op("GET_RANGE_DATABASE"), b(&all));
    // The last row: reverse, limit 1, in the transaction (whose read version predates the
    // SET_DATABASE: it sees foo1 only).
    s.int(-1);
    s.int(1);
    s.int(1);
    s.bytes(&ws);
    expect(
        s.op("GET_RANGE_STARTS_WITH"),
        b(&pack(vec![b(&foo1), b(b"bar")])),
    );
    s.op("NEW_TRANSACTION");
    // GET_KEY: KEY, OR_EQUAL, OFFSET, PREFIX: first_greater_than(foo1).
    s.bytes(&ws);
    s.int(1);
    s.int(1);
    s.bytes(&foo1);
    expect(s.op("GET_KEY"), b(&foo2));
    // A key selector before the workspace is clamped to its prefix.
    s.bytes(&ws);
    s.int(0);
    s.int(0);
    s.bytes(&ws);
    expect(s.op("GET_KEY_SNAPSHOT"), b(&ws));
    // GET_RANGE_SELECTOR, snapshot, serial mode, keys filtered by PREFIX (foo1's).
    s.bytes(&foo1);
    s.int(4);
    s.int(0);
    s.int(0);
    s.int(1);
    s.int(0);
    s.bytes(&ws_end);
    s.int(1);
    s.int(0);
    s.bytes(&ws);
    expect(
        s.op("GET_RANGE_SELECTOR_SNAPSHOT"),
        b(&pack(vec![b(&foo1), b(b"bar")])),
    );
    // Exact mode without a limit.
    s.int(0);
    s.int(0);
    s.int(0);
    s.bytes(&ws_end);
    s.bytes(&ws);
    expect(s.op("GET_RANGE"), error(2210));
    // An atomic add, in its own transaction.
    s.bytes(&1u64.to_le_bytes());
    s.bytes(&foo3);
    s.string("ADD");
    expect(s.op("ATOMIC_OP_DATABASE"), b(NOT_PRESENT));
    s.bytes(&foo3);
    expect(s.op("GET_DATABASE"), b(&1u64.to_le_bytes()));

    // A versionstamped key.
    s.op("NEW_TRANSACTION");
    let mut stamped = stamp_prefix.clone();
    stamped.extend([0u8; 10]);
    stamped.extend((stamp_prefix.len() as u32).to_le_bytes());
    s.bytes(b"stamped");
    s.bytes(&stamped);
    s.string("SET_VERSIONSTAMPED_KEY");
    s.op("ATOMIC_OP");
    let stamp = s.op("GET_VERSIONSTAMP");
    let commit = s.op("COMMIT");
    s.op("WAIT_FUTURE");
    expected.push((stamp, Expected::Stamp));
    expected.push((commit, Expected::Value(b(NOT_PRESENT))));
    let mut expect = |i: usize, v: Element| expected.push((i, Expected::Value(v)));
    s.op("RESET");

    // ON_ERROR: retried, then not.
    s.int(1020);
    expect(s.op("ON_ERROR"), b(NOT_PRESENT));
    s.op("WAIT_FUTURE");
    s.int(2000);
    expect(s.op("ON_ERROR"), error(2000));

    // A conflict between two named transactions.
    s.bytes(b"tr_a");
    s.op("USE_TRANSACTION");
    s.bytes(&foo1);
    expect(s.op("GET"), b(b"bar"));
    s.bytes(b"x");
    s.bytes(&foo4);
    s.op("SET");
    s.bytes(b"tr_b");
    s.op("USE_TRANSACTION");
    s.bytes(b"changed");
    s.bytes(&foo1);
    s.op("SET");
    expect(s.op("COMMIT"), b(NOT_PRESENT));
    s.op("WAIT_FUTURE");
    s.bytes(b"tr_a");
    s.op("USE_TRANSACTION");
    expect(s.op("COMMIT"), error(1020));
    s.op("WAIT_FUTURE");
    s.bytes(prefix);
    s.op("USE_TRANSACTION");

    // Tuples and arithmetic.
    s.bytes(b"b");
    s.int(1);
    s.int(2);
    expect(s.op("TUPLE_PACK"), b(&pack(vec![Element::Int(1), b(b"b")])));
    s.int(10);
    s.int(3);
    expect(s.op("SUB"), Element::Int(-7));

    // A thread that clears the wait key; WAIT_EMPTY waits for it.
    s.bytes(b"");
    s.bytes(&wait_key);
    s.op("SET_DATABASE");
    s.op("POP");
    s.bytes(thread_prefix);
    s.op("START_THREAD");
    s.bytes(&wait_prefix);
    expect(s.op("WAIT_EMPTY"), b(b"WAITED_FOR_EMPTY"));

    s.bytes(&results);
    s.op("LOG_STACK");

    let mut t = Script::default();
    t.op("NEW_TRANSACTION");
    t.bytes(b"threadvalue");
    t.bytes(&thread_key);
    t.op("SET");
    t.bytes(&wait_key);
    t.op("CLEAR");
    t.op("COMMIT");
    t.op("WAIT_FUTURE");
    t.op("POP");

    s.store(&db, prefix).await.unwrap();
    t.store(&db, thread_prefix).await.unwrap();

    let out = run_tester("stk_main", fdb.cluster_file()).await;
    assert_success(&out);

    let logged = logged_stack(&db, &results).await;
    assert_eq!(logged.len(), expected.len(), "{logged:?}");
    let mut stamp_value = None;
    for ((number, value), (expected_number, expected_value)) in logged.iter().zip(&expected) {
        assert_eq!(number, expected_number, "{value:?} vs {expected_value:?}");
        match expected_value {
            Expected::Value(v) => assert_eq!(value, v, "instruction {number}"),
            Expected::Stamp => {
                let Element::Bytes(stamp) = value else {
                    panic!("versionstamp {value:?}")
                };
                assert_eq!(stamp.len(), 10);
                stamp_value = Some(stamp.clone());
            }
        }
    }

    // What the instructions wrote.
    let stamped_rows = read_prefix(&db, &stamp_prefix).await;
    let mut stamped_key = stamp_prefix.clone();
    stamped_key.extend(stamp_value.unwrap());
    assert_eq!(stamped_rows, vec![(stamped_key, b"stamped".to_vec())]);
    let workspace = read_prefix(&db, &ws).await;
    let get = |k: &[u8]| {
        workspace
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(get(&foo1).as_deref(), Some(&b"changed"[..]));
    assert_eq!(get(&foo4), None, "the conflicting transaction committed");
    assert_eq!(get(&thread_key).as_deref(), Some(&b"threadvalue"[..]));
    assert_eq!(get(&wait_key), None);

    // An instruction the tester does not know is fatal: the exit status says so.
    let mut bad = Script::default();
    bad.int(1);
    bad.op("NO_SUCH_INSTRUCTION");
    bad.store(&db, b"stk_bad").await.unwrap();
    let out = run_tester("stk_bad", fdb.cluster_file()).await;
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("instruction 1 (NO_SUCH_INSTRUCTION)"),
        "{stderr}"
    );

    stop(fdb, db).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_stacktester_unit_tests() {
    let (fdb, db) = start().await;
    let results = b"stk/unit/".to_vec();
    let mut s = Script::default();
    s.bytes(b"sentinel");
    s.op("UNIT_TESTS");
    s.bytes(&results);
    s.op("LOG_STACK");
    s.store(&db, b"stk_unit").await.unwrap();

    let out = run_tester("stk_unit", fdb.cluster_file()).await;
    assert_success(&out);
    assert_eq!(logged_stack(&db, &results).await, vec![(0, b(b"sentinel"))]);
    stop(fdb, db).await;
}
