//! Docker test harness for running tests against disposable FoundationDB containers.
//!
//! Tests that need a live cluster must only ever target containers started by this crate. A
//! malformed packet can make an FDB 7.4/8.0 server process exit, so never point a test at a
//! shared or long-lived cluster.
//!
//! # Usage
//!
//! ```no_run
//! use fdb_native_testkit::FdbContainer;
//!
//! # fn main() -> Result<(), fdb_native_testkit::Error> {
//! // Image comes from `FDB_IMAGE`, falling back to `foundationdb/foundationdb:7.4.10`.
//! let fdb = FdbContainer::start()?;
//!
//! println!("cluster file:   {}", fdb.cluster_file().display());
//! println!("cluster string: {}", fdb.cluster_string()); // docker:docker@172.17.0.2:4500
//! println!("server address: {}", fdb.address());
//!
//! // ... connect a client using `fdb.cluster_file()` ...
//!
//! // The container is removed (`docker rm -f -v <id>`) when `fdb` is dropped.
//! # Ok(())
//! # }
//! ```
//!
//! Use [`FdbContainer::builder`] to choose the image explicitly, change the timeouts or
//! override the container name.
//!
//! # Blocking API
//!
//! Everything here is synchronous: [`FdbContainer::start`] blocks for several seconds (and
//! for as long as an image pull takes, if the image is missing) and **`Drop` blocks** while it
//! runs `docker rm`. In async tests, start the container before entering the runtime (for
//! example in a plain `fn main`/test body that then builds the runtime), or call `start()`
//! inside `tokio::task::spawn_blocking`. Avoid dropping the handle directly on an async
//! worker thread for the same reason.
//!
//! # Running tests
//!
//! Docker-backed tests should be marked `#[ignore = "requires docker"]` and are run with
//! `cargo nextest run --run-ignored only`. Under nextest every test runs in its own process,
//! so every test gets its own container (about 4 seconds each). To avoid overloading the
//! Docker daemon, the workspace's `.config/nextest.toml` puts Docker tests into a `docker`
//! test group capped at 4 concurrent tests. The group matches tests in an integration-test
//! binary named `docker`, or tests whose name starts with `docker_`; follow that convention
//! for new Docker-backed tests so they are capped too.
//!
//! # How it works (Linux)
//!
//! 1. Make sure the image exists locally (`docker image inspect`, else `docker pull`, with its
//!    own timeout).
//! 2. `docker create --network bridge --label fdb-native-testkit=1 --name
//!    fdb-native-test-<pid>-<counter>-<nanos> <image>` with the image's default entrypoint
//!    (tini -> `fdb.bash`) and **no command arguments**. The printed container ID goes into a
//!    drop guard immediately, then `docker start <id>` runs it. The crate only ever refers to
//!    the container by that ID, never by name, so a name collision cannot make it touch
//!    someone else's container.
//! 3. Read the container's bridge IP with `docker inspect`. On Linux the host can route
//!    straight to the bridge network, so the server is reachable at `<ip>:4500`.
//! 4. Read the authoritative cluster file from the container (`/var/fdb/fdb.cluster`), check
//!    it is `docker:docker@<ip>:4500`, and write it to a temporary directory owned by the
//!    handle.
//! 5. Run `fdbcli --exec "configure new single memory"` in the container, retrying until the
//!    server accepts it, then poll `fdbcli --exec "status minimal"` until the database is
//!    available, then verify the host can open a TCP connection to `<ip>:4500`. The
//!    readiness phase has an overall timeout (default 60 seconds) that starts after
//!    `docker start`, and it fails fast, with the container logs, if the container exits.
//!
//! Everything shells out to the `docker` CLI; there is no Docker API client dependency. Every
//! external command has a hard timeout, after which the process is killed.
//!
//! # Image behaviour (verified against 7.4.10 and 8.0.1)
//!
//! Both images have entrypoint `["/usr/bin/tini","-g","--","/var/fdb/scripts/fdb.bash"]`, no
//! `Cmd`, and `FDB_CLUSTER_FILE=/var/fdb/fdb.cluster`. The 7.4 `fdb.bash` ignores any command
//! arguments; the 8.0.1 `fdb.bash` appends them (`"$@"`) to the `fdbserver` command line. In
//! both cases this crate passes none. Both images declare `VOLUME /var/fdb/data`, which makes
//! Docker create an anonymous volume per container; that is why containers are removed with
//! `docker rm -f -v`.
//!
//! # Cleanup guarantees
//!
//! The container is removed by exact container ID when the [`FdbContainer`] is dropped, and
//! also if startup fails part-way. Removal errors are logged to stderr and never panic. The
//! crate never removes containers by name, name pattern, label or filter. If the process is
//! killed (SIGKILL, abort) the container is leaked; it can be found with
//! `docker ps -a --filter label=fdb-native-testkit=1` and removed by ID.
//!
//! # Docker Desktop (macOS / Windows): not implemented
//!
//! On Docker Desktop the container bridge IP is not routable from the host, so the approach
//! above does not work (startup fails with [`Error::Unreachable`]). The alternative is to
//! publish the port (`-p 4500:4500`) and start the container with
//! `-e FDB_NETWORKING_MODE=host`. Reading both images' `fdb.bash` confirms that in host mode
//! the server advertises `127.0.0.1:4500` as its public address and writes a matching
//! self-coordinated cluster file (`docker:docker@127.0.0.1:4500`) while still listening on
//! `0.0.0.0:4500`, so the mapped port is reachable from the host. The 8.0.1 script also honours
//! `FDB_PUBLIC_IP` in container mode (7.4 does not). The end-to-end Docker Desktop setup itself
//! is unverified here. `FDB_COORDINATOR` is not a public-address setting: it is a hostname of
//! an existing coordinator to join. Only the Linux path is implemented today.
//!
//! # TLS clusters
//!
//! [`FdbContainerBuilder::tls`] starts the same single-process server with TLS on. The
//! certificates come from [`TestPki`] (a CA, a server certificate and a client certificate,
//! generated in process), are written to a temporary directory and bind-mounted **read-only**
//! at [`CONTAINER_TLS_DIR`] (`/etc/fdb-tls`). The server is configured with environment
//! variables on `docker create`, which the container's `fdbcli` (run by `docker exec`) inherits:
//!
//! | Variable | Value |
//! |----------|-------|
//! | `FDB_PORT` | `4500:tls` |
//! | `FDB_TLS_CERTIFICATE_FILE` | `/etc/fdb-tls/server-cert.pem` |
//! | `FDB_TLS_KEY_FILE` | `/etc/fdb-tls/server-key.pem` |
//! | `FDB_TLS_CA_FILE` | `/etc/fdb-tls/ca.pem` |
//! | `FDB_TLS_VERIFY_PEERS` | only if [`FdbContainerBuilder::tls_verify_peers`] is set |
//!
//! `FDB_PORT` is the only switch that matters: both images' `fdb.bash` use it verbatim in
//! `--listen-address 0.0.0.0:$FDB_PORT`, in `--public-address $PUBLIC_IP:$FDB_PORT` and in the
//! self-coordinated cluster file, so `4500:tls` makes the process listen for TLS, advertise
//! `IP:4500:tls` and write `docker:docker@IP:4500:tls`. No command arguments are needed (the 7.4
//! script would ignore them; the 8.0 script appends them). The same file is the cluster file
//! the host uses, and the server certificate doubles as `fdbcli`'s client certificate (it is
//! valid for client authentication too).
//!
//! The server asks clients for a certificate but accepts none (an *untrusted* client may use
//! public endpoints only); a client that presents one has it verified against the CA.
//!
//! A [`FdbContainer`] started this way has [`FdbContainer::tls`], the handle with the client
//! certificate and CA as paths and PEM text. [`FdbContainer::address`] is still the plain
//! `IP:4500`; the address a client dials is that with the TLS flag, and
//! [`FdbContainer::cluster_string`] ends in `:tls`.
//!
//! # Multi-process clusters
//!
//! [`FdbCluster`] starts several `fdbserver` containers that form one cluster, for fault tests
//! (kill, pause, exclude, coordinator changes, shard moves). See the [`cluster`] module docs for
//! the recipe, the per-image details and the API.

use std::env;
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

pub mod cluster;
pub mod tls;

pub use cluster::{
    ClusterHealth, FdbCluster, FdbClusterBuilder, Redundancy, StorageEngine, serde_json,
};
pub use tls::{CONTAINER_TLS_DIR, Identity, TestPki, TlsCluster};

/// Environment variable selecting the Docker image.
pub const IMAGE_ENV: &str = "FDB_IMAGE";

/// Image used when [`IMAGE_ENV`] is unset or empty.
pub const DEFAULT_IMAGE: &str = "foundationdb/foundationdb:7.4.10";

/// Label put on every container created by this crate.
pub const LABEL: &str = "fdb-native-testkit=1";

/// Port the FDB server listens on inside the container.
pub const FDB_PORT: u16 = 4500;

/// Default timeout for configuring the database and waiting for readiness after
/// `docker start`.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// Default timeout for making sure the image is available locally (including any pull).
pub const DEFAULT_PULL_TIMEOUT: Duration = Duration::from_secs(300);

const DOCKER: &str = "docker";
const NAME_PREFIX: &str = "fdb-native-test";
/// Where the image's `fdb.bash` writes the cluster file (`FDB_CLUSTER_FILE`).
const CONTAINER_CLUSTER_FILE: &str = "/var/fdb/fdb.cluster";
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Hard bound for ordinary docker commands (create, start, inspect, logs).
const DOCKER_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard bound for `docker rm -f -v`.
const REMOVE_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard bound for a `docker exec ... fdbcli` call; above fdbcli's own timeout.
const EXEC_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-invocation fdbcli timeout, in seconds.
const FDBCLI_TIMEOUT_SECS: &str = "5";
const TCP_TIMEOUT: Duration = Duration::from_secs(2);

/// Errors produced while starting or talking to a disposable FDB container.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The `docker` binary could not be executed or waited on.
    #[error("failed to execute `docker`: {0}")]
    Spawn(#[source] io::Error),
    /// A docker command ran but exited unsuccessfully.
    #[error("`docker {command}` failed ({status}): {stderr}")]
    Docker {
        /// The docker arguments (without the leading `docker`).
        command: String,
        /// Exit status description.
        status: String,
        /// Trimmed standard error output.
        stderr: String,
    },
    /// A docker command did not finish in time and was killed.
    #[error("`docker {command}` did not finish within {timeout:?} and was killed")]
    CommandTimeout {
        /// The docker arguments (without the leading `docker`).
        command: String,
        /// The timeout that elapsed.
        timeout: Duration,
    },
    /// The output of a docker command could not be understood.
    #[error("unexpected docker output: {0}")]
    Parse(String),
    /// The cluster file could not be created.
    #[error("failed to write cluster file: {0}")]
    ClusterFile(#[source] io::Error),
    /// The test certificates could not be generated or written.
    #[error("failed to generate or write TLS test certificates: {0}")]
    Pki(String),
    /// The cluster file inside the container is not what the bridge IP implies.
    #[error("cluster file in container is {actual:?}, expected {expected:?}")]
    ClusterFileMismatch {
        /// What the inspected container IP implies.
        expected: String,
        /// What the container's cluster file contains.
        actual: String,
    },
    /// The container stopped running during startup.
    #[error("FoundationDB container {container} exited during startup; logs: {logs:?}")]
    ContainerExited {
        /// The container ID.
        container: String,
        /// The tail of the container logs (best effort).
        logs: String,
    },
    /// The database did not become available before the deadline.
    #[error(
        "FoundationDB container {container} was not ready within {timeout:?} \
         (last output: {last_output:?}; container logs: {logs:?})"
    )]
    NotReady {
        /// The container ID.
        container: String,
        /// The timeout that elapsed.
        timeout: Duration,
        /// The last output seen from the container.
        last_output: String,
        /// The tail of the container logs (best effort).
        logs: String,
    },
    /// The database is up but the host cannot reach it at the container's bridge IP.
    #[error(
        "bridge IP not routable from host: cannot connect to {address} ({source}); \
         see the Docker Desktop section of the fdb-native-testkit docs"
    )]
    Unreachable {
        /// The address that could not be reached.
        address: SocketAddr,
        /// The connection error.
        #[source]
        source: io::Error,
    },
    /// The container came back with another bridge IP after a stop or restart, so the cluster
    /// file and every address derived from it are stale.
    #[error("container {container} came back at {after} instead of {before}")]
    AddressChanged {
        /// The container ID.
        container: String,
        /// The address before.
        before: IpAddr,
        /// The address after.
        after: IpAddr,
    },
    /// A cluster was configured impossibly (process count, coordinator choice, ...).
    #[error("invalid cluster configuration: {0}")]
    InvalidConfig(String),
    /// An `fdbcli` command ran but did not do what was asked.
    #[error("fdbcli `{command}` did not succeed: {output:?}")]
    Fdbcli {
        /// The fdbcli command.
        command: String,
        /// Its (trimmed) output.
        output: String,
    },
    /// `status json` could not be fetched or understood.
    #[error("cannot read cluster status: {0}")]
    Status(String),
    /// A multi-process cluster did not reach the wanted state before the deadline.
    #[error("cluster not {wanted} within {timeout:?} (last state: {last})")]
    ClusterNotReady {
        /// What was being waited for.
        wanted: &'static str,
        /// The timeout that elapsed.
        timeout: Duration,
        /// A summary of the last state seen.
        last: String,
    },
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------------------

/// Resolves the image from an optional `FDB_IMAGE` value.
fn resolve_image(env_value: Option<&str>) -> String {
    match env_value.map(str::trim) {
        Some(v) if !v.is_empty() => v.to_owned(),
        _ => DEFAULT_IMAGE.to_owned(),
    }
}

/// Returns the default image: `$FDB_IMAGE`, or [`DEFAULT_IMAGE`].
pub fn default_image() -> String {
    resolve_image(env::var(IMAGE_ENV).ok().as_deref())
}

/// Formats the cluster string for a container reachable at `addr`.
fn format_cluster_string(addr: SocketAddr) -> String {
    format!("docker:docker@{addr}")
}

/// Builds a unique container name from its parts.
fn format_container_name(pid: u32, counter: u64, nanos: u128) -> String {
    format!("{NAME_PREFIX}-{pid}-{counter}-{nanos:x}")
}

fn generate_container_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format_container_name(std::process::id(), counter, nanos)
}

/// Go template giving one `<network name> <ip>` line per attached network.
const INSPECT_IP_TEMPLATE: &str = "{{range $name, $net := .NetworkSettings.Networks}}{{$name}} {{$net.IPAddress}}{{\"\\n\"}}{{end}}";

/// Parses the output of `docker inspect -f <INSPECT_IP_TEMPLATE>`.
///
/// The container is created on the default `bridge` network, so that network's address is
/// preferred; otherwise the first network with a parsable address is used.
fn parse_inspect_ip(output: &str) -> Option<IpAddr> {
    let mut first = None;
    for line in output.lines() {
        let mut parts = line.split_whitespace();
        let (Some(name), Some(ip)) = (parts.next(), parts.next()) else {
            continue;
        };
        let Ok(ip) = ip.parse::<IpAddr>() else {
            continue;
        };
        if name == "bridge" {
            return Some(ip);
        }
        first.get_or_insert(ip);
    }
    first
}

/// Validates and returns a container ID printed by `docker create`: exactly 64 hex digits.
fn parse_container_id(output: &str) -> Option<String> {
    // The ID is the last non-empty line; earlier lines could be warnings.
    let id = output.lines().map(str::trim).rfind(|l| !l.is_empty())?;
    let valid = id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit());
    valid.then(|| id.to_owned())
}

/// Whether `fdbcli` output to `status minimal` says the database is usable.
fn status_is_available(output: &str) -> bool {
    output.contains("The database is available")
}

/// Whether `configure new ...` output means the database is configured (now or previously).
fn configure_succeeded(output: &str) -> bool {
    output.contains("Database created") || output.contains("Database already exists")
}

// ---------------------------------------------------------------------------------------
// Docker plumbing
// ---------------------------------------------------------------------------------------

/// Runs `docker <args>` with a hard timeout (the process is killed on expiry) and returns the
/// raw output regardless of exit status.
fn docker_raw(args: &[&str], timeout: Duration) -> Result<Output> {
    let mut cmd = Command::new(DOCKER);
    cmd.args(args);
    run_bounded(cmd, &args.join(" "), timeout)
}

/// Runs `cmd`, killing it if it has not exited after `timeout`.
fn run_bounded(cmd: Command, label: &str, timeout: Duration) -> Result<Output> {
    run_bounded_input(cmd, label, timeout, None)
}

/// Like [`run_bounded`], feeding `input` to the command's standard input (then closing it).
fn run_bounded_input(
    mut cmd: Command,
    label: &str,
    timeout: Duration,
    input: Option<Vec<u8>>,
) -> Result<Output> {
    let mut child = cmd
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(Error::Spawn)?;
    // Written on its own thread so a command that stops reading cannot block the deadline loop;
    // the pipe is closed when the thread ends (or when the child is killed).
    let writer = input.zip(child.stdin.take()).map(|(input, mut stdin)| {
        thread::spawn(move || {
            // Best effort: the command may exit without reading everything.
            let _ = stdin.write_all(&input);
        })
    });

    fn drain<R: Read + Send + 'static>(pipe: Option<R>) -> thread::JoinHandle<Vec<u8>> {
        thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(mut pipe) = pipe {
                // Best effort: a read error just truncates the captured output.
                let _ = pipe.read_to_end(&mut buf);
            }
            buf
        })
    }
    let out_reader = drain(child.stdout.take());
    let err_reader = drain(child.stderr.take());

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if Instant::now() >= deadline => {
                // Kill failures mean it already exited; either way reap it.
                let _ = child.kill();
                let _ = child.wait();
                break Err(Error::CommandTimeout {
                    command: label.to_owned(),
                    timeout,
                });
            }
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(Error::Spawn(e));
            }
        }
    };
    if let Some(writer) = writer {
        let _ = writer.join();
    }
    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    Ok(Output {
        status: status?,
        stdout,
        stderr,
    })
}

/// Runs `docker <args>` with `input` on stdin and a hard timeout; returns the raw output.
fn docker_raw_input(args: &[&str], input: Vec<u8>, timeout: Duration) -> Result<Output> {
    let mut cmd = Command::new(DOCKER);
    cmd.args(args);
    run_bounded_input(cmd, &args.join(" "), timeout, Some(input))
}

/// Runs `docker <args>`, requiring success, and returns stdout.
fn docker(args: &[&str], timeout: Duration) -> Result<String> {
    let out = docker_raw(args, timeout)?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(Error::Docker {
            command: args.join(" "),
            status: out.status.to_string(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        })
    }
}

/// Runs `fdbcli --exec <command>` in the container and returns stdout + stderr regardless of
/// exit status.
fn fdbcli_lenient(id: &str, command: &str) -> Result<String> {
    let out = docker_raw(
        &[
            "exec",
            id,
            "fdbcli",
            "--timeout",
            FDBCLI_TIMEOUT_SECS,
            "--exec",
            command,
        ],
        EXEC_TIMEOUT,
    )?;
    let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
    s.push_str(&String::from_utf8_lossy(&out.stderr));
    Ok(s)
}

/// Best-effort tail of the container logs.
fn log_tail(id: &str) -> String {
    docker(&["logs", "--tail", "20", id], DOCKER_TIMEOUT)
        .unwrap_or_else(|e| format!("<unavailable: {e}>"))
}

/// Writes a diagnostic line to stderr without ever panicking (unlike `eprintln!`).
fn log_stderr(args: fmt::Arguments<'_>) {
    let _ = writeln!(io::stderr(), "fdb-native-testkit: {args}");
}

/// Owns a container ID and force-removes exactly that container (and its anonymous volumes)
/// on drop.
#[derive(Debug)]
struct ContainerGuard {
    id: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        match docker_raw(&["rm", "-f", "-v", &self.id], REMOVE_TIMEOUT) {
            Ok(out) if out.status.success() => {}
            Ok(out) => log_stderr(format_args!(
                "`docker rm -f -v {}` failed ({}): {}",
                self.id,
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            Err(e) => log_stderr(format_args!("could not remove container {}: {e}", self.id)),
        }
    }
}

/// Makes sure `image` exists locally, pulling it if necessary.
fn ensure_image(image: &str, pull_timeout: Duration) -> Result<()> {
    let present = docker_raw(
        &["image", "inspect", "-f", "{{.Id}}", image],
        DOCKER_TIMEOUT,
    )?
    .status
    .success();
    if !present {
        docker(&["pull", image], pull_timeout)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------------------

/// Builder for [`FdbContainer`].
#[derive(Debug, Clone)]
pub struct FdbContainerBuilder {
    image: String,
    startup_timeout: Duration,
    pull_timeout: Duration,
    name: Option<String>,
    tls: bool,
    tls_verify_peers: Option<String>,
}

impl Default for FdbContainerBuilder {
    fn default() -> Self {
        Self {
            image: default_image(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            pull_timeout: DEFAULT_PULL_TIMEOUT,
            name: None,
            tls: false,
            tls_verify_peers: None,
        }
    }
}

impl FdbContainerBuilder {
    /// Sets the Docker image (default: `$FDB_IMAGE` or [`DEFAULT_IMAGE`]).
    #[must_use]
    pub fn image(mut self, image: impl Into<String>) -> Self {
        self.image = image.into();
        self
    }

    /// Sets the timeout for configuring the database and waiting for readiness. It starts
    /// after the container has been started, so it does not include any image pull.
    #[must_use]
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Sets the timeout for making the image available locally (default 300 seconds).
    #[must_use]
    pub fn pull_timeout(mut self, timeout: Duration) -> Self {
        self.pull_timeout = timeout;
        self
    }

    /// Overrides the container name. It must be unique: if a container with this name
    /// exists, `start` fails and leaves that container alone.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Starts the server with TLS on: it listens on and advertises `IP:4500:tls`, with
    /// generated certificates (see the [crate docs](crate#tls-clusters)). The handle then has
    /// [`FdbContainer::tls`].
    #[must_use]
    pub fn tls(mut self) -> Self {
        self.tls = true;
        self
    }

    /// Sets the server's `FDB_TLS_VERIFY_PEERS` (how it judges client certificates); implies
    /// [`tls`](Self::tls).
    #[must_use]
    pub fn tls_verify_peers(mut self, rules: impl Into<String>) -> Self {
        self.tls = true;
        self.tls_verify_peers = Some(rules.into());
        self
    }

    /// Starts the container and waits until the database is available.
    pub fn start(self) -> Result<FdbContainer> {
        let name = self.name.unwrap_or_else(generate_container_name);

        // Certificates first, so a failure here creates nothing.
        let tls = if self.tls {
            let pki = TestPki::generate()?;
            Some(TlsCluster::new(pki).map_err(|e| Error::Pki(e.to_string()))?)
        } else {
            None
        };

        ensure_image(&self.image, self.pull_timeout)?;

        let mut create: Vec<String> = [
            "create",
            "--network",
            "bridge",
            "--label",
            LABEL,
            "--name",
            &name,
        ]
        .map(str::to_owned)
        .into();
        if let Some(tls) = &tls {
            let dir = CONTAINER_TLS_DIR;
            let mount = format!("{}:{dir}:ro", tls.dir().display());
            let mut envs = vec![
                format!("FDB_PORT={FDB_PORT}:tls"),
                format!("FDB_TLS_CERTIFICATE_FILE={dir}/server-cert.pem"),
                format!("FDB_TLS_KEY_FILE={dir}/server-key.pem"),
                format!("FDB_TLS_CA_FILE={dir}/ca.pem"),
            ];
            if let Some(rules) = &self.tls_verify_peers {
                envs.push(format!("FDB_TLS_VERIFY_PEERS={rules}"));
            }
            create.extend(["--volume".to_owned(), mount]);
            for env in envs {
                create.extend(["--env".to_owned(), env]);
            }
        }
        create.push(self.image.clone());
        let create_args: Vec<&str> = create.iter().map(String::as_str).collect();

        // `docker create` either fails (nothing exists, nothing to clean up, and in particular
        // a name collision never involves the other container) or prints the new ID.
        let created = docker(&create_args, DOCKER_TIMEOUT)?;
        let id = parse_container_id(&created).ok_or_else(|| {
            Error::Parse(format!(
                "docker create printed no container id: {created:?}"
            ))
        })?;
        // From here on the guard guarantees removal of exactly this container, including on
        // early return.
        let guard = ContainerGuard { id };

        docker(&["start", &guard.id], DOCKER_TIMEOUT)?;
        let deadline = Instant::now() + self.startup_timeout;

        let ip_out = docker(
            &["inspect", "-f", INSPECT_IP_TEMPLATE, &guard.id],
            DOCKER_TIMEOUT,
        )?;
        let ip = parse_inspect_ip(&ip_out).ok_or_else(|| {
            Error::Parse(format!(
                "no container IP address in inspect output {ip_out:?}"
            ))
        })?;
        let address = SocketAddr::new(ip, FDB_PORT);
        let expected = if tls.is_some() {
            format!("{}:tls", format_cluster_string(address))
        } else {
            format_cluster_string(address)
        };

        let cluster_string = wait_ready(
            &guard.id,
            address,
            &expected,
            self.startup_timeout,
            deadline,
        )?;

        let dir = tempfile::Builder::new()
            .prefix("fdb-native-testkit-")
            .tempdir()
            .map_err(Error::ClusterFile)?;
        let cluster_file = dir.path().join("fdb.cluster");
        fs::write(&cluster_file, format!("{cluster_string}\n")).map_err(Error::ClusterFile)?;

        Ok(FdbContainer {
            guard,
            image: self.image,
            name,
            address,
            cluster_string,
            cluster_file,
            tls,
            _dir: dir,
        })
    }
}

/// Steps of the readiness sequence; each poll iteration performs exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    ClusterFile,
    Configure,
    Status,
    Reachable,
}

/// Waits for the server and returns the verified cluster string.
fn wait_ready(
    id: &str,
    address: SocketAddr,
    expected: &str,
    timeout: Duration,
    deadline: Instant,
) -> Result<String> {
    let mut phase = Phase::ClusterFile;
    let mut cluster_string = String::new();
    let mut last_output = String::new();
    let mut last_connect_error = None;

    // At least one iteration always runs, whatever the timeout.
    loop {
        let running = docker(&["inspect", "-f", "{{.State.Running}}", id], DOCKER_TIMEOUT)?;
        if running.trim() != "true" {
            return Err(Error::ContainerExited {
                container: id.to_owned(),
                logs: log_tail(id),
            });
        }

        let advanced = match phase {
            Phase::ClusterFile => {
                // Not yet written (or exec failing) is not an error; keep polling.
                match docker_raw(&["exec", id, "cat", CONTAINER_CLUSTER_FILE], EXEC_TIMEOUT)? {
                    out if out.status.success() => {
                        let contents = String::from_utf8_lossy(&out.stdout).trim().to_owned();
                        if contents.is_empty() {
                            false
                        } else if contents == expected {
                            cluster_string = contents;
                            true
                        } else {
                            return Err(Error::ClusterFileMismatch {
                                expected: expected.to_owned(),
                                actual: contents,
                            });
                        }
                    }
                    out => {
                        last_output = String::from_utf8_lossy(&out.stderr).trim().to_owned();
                        false
                    }
                }
            }
            Phase::Configure => {
                let out = fdbcli_lenient(id, "configure new single memory")?;
                let ok = configure_succeeded(&out);
                last_output = out;
                ok
            }
            Phase::Status => {
                let out = fdbcli_lenient(id, "status minimal")?;
                let ok = status_is_available(&out);
                last_output = out;
                ok
            }
            Phase::Reachable => match TcpStream::connect_timeout(&address, TCP_TIMEOUT) {
                Ok(_) => return Ok(cluster_string),
                Err(e) => {
                    last_output = format!("host cannot connect to {address}: {e}");
                    last_connect_error = Some(e);
                    false
                }
            },
        };

        if advanced {
            phase = match phase {
                Phase::ClusterFile => Phase::Configure,
                Phase::Configure => Phase::Status,
                Phase::Status | Phase::Reachable => Phase::Reachable,
            };
        }

        if Instant::now() >= deadline {
            // A database that is up but unreachable from the host is a routing problem.
            if phase == Phase::Reachable {
                if let Some(source) = last_connect_error {
                    return Err(Error::Unreachable { address, source });
                }
            }
            return Err(Error::NotReady {
                container: id.to_owned(),
                timeout,
                last_output: last_output.trim().to_owned(),
                logs: log_tail(id),
            });
        }
        if !advanced {
            thread::sleep(POLL_INTERVAL);
        }
    }
}

/// A disposable single-process, in-memory FoundationDB server running in a Docker container.
///
/// The container is removed when this handle is dropped (which blocks briefly). See the
/// [crate docs](crate).
#[derive(Debug)]
pub struct FdbContainer {
    // Field order matters for drop order: remove the container before the temp dir.
    guard: ContainerGuard,
    image: String,
    name: String,
    address: SocketAddr,
    cluster_string: String,
    cluster_file: PathBuf,
    // Dropped after the guard: the container is gone before its mounted certificates are.
    tls: Option<TlsCluster>,
    _dir: TempDir,
}

impl FdbContainer {
    /// Starts a container using [`default_image`] and the default timeouts. Blocking.
    pub fn start() -> Result<Self> {
        Self::builder().start()
    }

    /// Starts a container for `image` with the default timeouts. Blocking.
    pub fn start_with_image(image: impl Into<String>) -> Result<Self> {
        Self::builder().image(image).start()
    }

    /// Starts a TLS container for `image` with the default timeouts. Blocking. See
    /// [`FdbContainerBuilder::tls`].
    pub fn start_tls_with_image(image: impl Into<String>) -> Result<Self> {
        Self::builder().image(image).tls().start()
    }

    /// The TLS side of a container started with [`FdbContainerBuilder::tls`]: the generated
    /// certificates, with the client's as paths and PEM text. `None` for a plain container.
    pub fn tls(&self) -> Option<&TlsCluster> {
        self.tls.as_ref()
    }

    /// Whether the server speaks TLS: clients must dial [`address`](Self::address) as a
    /// `:tls` address.
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Returns a builder for custom configuration.
    pub fn builder() -> FdbContainerBuilder {
        FdbContainerBuilder::default()
    }

    /// Path of the cluster file (owned by this handle; deleted on drop).
    pub fn cluster_file(&self) -> &Path {
        &self.cluster_file
    }

    /// The cluster string, e.g. `docker:docker@172.17.0.2:4500`.
    pub fn cluster_string(&self) -> &str {
        &self.cluster_string
    }

    /// The address of the FDB server (container bridge IP and port 4500).
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The full container ID.
    pub fn id(&self) -> &str {
        &self.guard.id
    }

    /// The container name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The image the container was started from.
    pub fn image(&self) -> &str {
        &self.image
    }

    /// Restarts the container (`docker restart` by exact ID, with a 1 s grace period): the
    /// server process dies, every TCP connection to it is reset, and the call returns once the
    /// database is available again at the same address. Blocking.
    ///
    /// The data directory is a volume, so an in-memory database recovers its data. Fails with
    /// [`Error::AddressChanged`] if Docker gave the container another IP address (the handle's
    /// [`address`](Self::address) and cluster file would then be stale; nothing here rewrites
    /// them).
    pub fn restart(&self) -> Result<()> {
        docker(&["restart", "-t", "1", &self.guard.id], DOCKER_TIMEOUT)?;
        self.wait_back()
    }

    /// Stops the container (`docker stop` by exact ID, with a 1 s grace period) and leaves it
    /// stopped: connecting to [`address`](Self::address) then fails (no route to host, or a
    /// timeout) until [`start_stopped`](Self::start_stopped). Blocking.
    pub fn stop(&self) -> Result<()> {
        docker(&["stop", "-t", "1", &self.guard.id], DOCKER_TIMEOUT).map(drop)
    }

    /// Starts a container stopped with [`stop`](Self::stop) (`docker start` by exact ID) and
    /// returns once the database is available again at the same address, as for
    /// [`restart`](Self::restart). Blocking.
    pub fn start_stopped(&self) -> Result<()> {
        docker(&["start", &self.guard.id], DOCKER_TIMEOUT)?;
        self.wait_back()
    }

    /// Freezes every process in the container (`docker pause` by exact ID, the cgroup
    /// freezer): the server stops answering but its sockets stay open, so a client sees a
    /// stalled peer (no replies, no pings) instead of a closed one. Undo with
    /// [`unpause`](Self::unpause). Blocking.
    pub fn pause(&self) -> Result<()> {
        docker(&["pause", &self.guard.id], DOCKER_TIMEOUT).map(drop)
    }

    /// Thaws a container frozen with [`pause`](Self::pause) (`docker unpause` by exact ID).
    /// Blocking.
    pub fn unpause(&self) -> Result<()> {
        docker(&["unpause", &self.guard.id], DOCKER_TIMEOUT).map(drop)
    }

    /// Waits until a started container serves the database at its recorded address again.
    fn wait_back(&self) -> Result<()> {
        let ip_out = docker(
            &["inspect", "-f", INSPECT_IP_TEMPLATE, &self.guard.id],
            DOCKER_TIMEOUT,
        )?;
        let after = parse_inspect_ip(&ip_out).ok_or_else(|| {
            Error::Parse(format!(
                "no container IP address in inspect output {ip_out:?}"
            ))
        })?;
        if after != self.address.ip() {
            return Err(Error::AddressChanged {
                container: self.guard.id.clone(),
                before: self.address.ip(),
                after,
            });
        }
        wait_ready(
            &self.guard.id,
            self.address,
            &self.cluster_string,
            DEFAULT_STARTUP_TIMEOUT,
            Instant::now() + DEFAULT_STARTUP_TIMEOUT,
        )
        .map(drop)
    }

    /// Runs `fdbcli --exec <command>` inside the container and returns its stdout.
    ///
    /// Fails if fdbcli exits non-zero.
    pub fn fdbcli(&self, command: &str) -> Result<String> {
        docker(
            &[
                "exec",
                &self.guard.id,
                "fdbcli",
                "--timeout",
                FDBCLI_TIMEOUT_SECS,
                "--exec",
                command,
            ],
            EXEC_TIMEOUT,
        )
    }
}

impl fmt::Display for FdbContainer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}, {})", self.name, self.image, self.cluster_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn image_resolution() {
        assert_eq!(resolve_image(None), DEFAULT_IMAGE);
        assert_eq!(resolve_image(Some("")), DEFAULT_IMAGE);
        assert_eq!(resolve_image(Some("  ")), DEFAULT_IMAGE);
        assert_eq!(
            resolve_image(Some(" foundationdb/foundationdb:8.0.1\n")),
            "foundationdb/foundationdb:8.0.1"
        );
    }

    #[test]
    fn cluster_string_format() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(172, 17, 0, 2), FDB_PORT));
        assert_eq!(format_cluster_string(addr), "docker:docker@172.17.0.2:4500");
    }

    #[test]
    fn name_format_and_uniqueness() {
        assert_eq!(
            format_container_name(42, 7, 0xabc),
            "fdb-native-test-42-7-abc"
        );
        let a = generate_container_name();
        let b = generate_container_name();
        assert_ne!(a, b);
        assert!(a.starts_with(&format!("{NAME_PREFIX}-{}-", std::process::id())));
        // Docker container names: [a-zA-Z0-9][a-zA-Z0-9_.-]*
        assert!(a.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'));
    }

    #[test]
    fn inspect_ip_parsing() {
        assert_eq!(
            parse_inspect_ip("bridge 172.17.0.2\n"),
            Some(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 2)))
        );
        // The default bridge network wins over other networks, wherever it appears.
        assert_eq!(
            parse_inspect_ip("custom 10.0.0.5\nbridge 172.17.0.3\n"),
            Some(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)))
        );
        // Without a bridge network, the first parsable address is used; empty ones skipped.
        assert_eq!(
            parse_inspect_ip("none \ncustom 10.0.0.5\nother 172.18.0.3\n"),
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)))
        );
        assert_eq!(parse_inspect_ip(""), None);
        assert_eq!(parse_inspect_ip("\n\n"), None);
        assert_eq!(parse_inspect_ip("bridge \n"), None);
        assert_eq!(parse_inspect_ip("bridge not-an-ip\n"), None);
    }

    #[test]
    fn container_id_parsing() {
        let id = "a1".repeat(32);
        assert_eq!(parse_container_id(&format!("{id}\n")), Some(id.clone()));
        assert_eq!(
            parse_container_id(&format!("WARNING: something\n{id}\n")),
            Some(id.clone())
        );
        assert_eq!(parse_container_id(""), None);
        assert_eq!(parse_container_id("Error response from daemon\n"), None);
        assert_eq!(parse_container_id("abc"), None);
        // Exactly 64 hex digits: a short prefix or an over-long string is rejected.
        assert_eq!(parse_container_id(&id[..63]), None);
        assert_eq!(parse_container_id(&format!("{id}0")), None);
        assert_eq!(parse_container_id(&"g".repeat(64)), None);
    }

    #[test]
    fn fdbcli_output_classification() {
        assert!(status_is_available("The database is available.\n"));
        assert!(!status_is_available("The database is unavailable\n"));
        assert!(!status_is_available(""));
        assert!(configure_succeeded("Database created\n"));
        assert!(configure_succeeded(
            "ERROR: Database already exists! To change configuration, don't say `new'\n"
        ));
        assert!(!configure_succeeded(
            "ERROR: Unable to connect to cluster from `/var/fdb/fdb.cluster'"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn bounded_run_kills_hung_command() {
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        let start = Instant::now();
        let err = run_bounded(cmd, "sleep 30", Duration::from_millis(200)).unwrap_err();
        assert!(matches!(err, Error::CommandTimeout { .. }), "got {err:?}");
        assert!(start.elapsed() < Duration::from_secs(10));
    }

    #[cfg(unix)]
    #[test]
    fn bounded_run_captures_output() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo out; echo err >&2; exit 3"]);
        let out = run_bounded(cmd, "sh", Duration::from_secs(30)).unwrap();
        assert_eq!(out.status.code(), Some(3));
        assert_eq!(out.stdout, b"out\n");
        assert_eq!(out.stderr, b"err\n");
    }
}
