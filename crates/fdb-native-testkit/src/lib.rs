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
//! // The container is removed (`docker rm -f <id>`) when `fdb` is dropped.
//! # Ok(())
//! # }
//! ```
//!
//! Use [`FdbContainer::builder`] to choose the image explicitly, change the startup timeout
//! or override the container name.
//!
//! Docker-backed tests should be marked `#[ignore = "requires docker"]` and are run with
//! `cargo nextest run --run-ignored only`.
//!
//! # How it works (Linux)
//!
//! 1. `docker run -d` the image with its default entrypoint (tini -> `fdb.bash`), a unique
//!    name (`fdb-native-test-<pid>-<counter>-<nanos>`) and the label `fdb-native-testkit=1`.
//!    No command arguments are passed; they are ignored by the 7.4 images.
//! 2. Read the container's bridge IP with `docker inspect`. On Linux the host can route
//!    straight to the bridge network, so the server is reachable at `<ip>:4500`.
//! 3. Write a cluster file containing `docker:docker@<ip>:4500` into a temporary directory
//!    owned by the handle.
//! 4. Run `docker exec <id> fdbcli --exec "configure new single memory"`, retrying until the
//!    server accepts it.
//! 5. Poll `docker exec <id> fdbcli --exec "status minimal"` until the database reports as
//!    available, within an overall timeout (default 60 seconds).
//!
//! Everything shells out to the `docker` CLI; there is no Docker API client dependency.
//!
//! # Cleanup guarantees
//!
//! The container is removed by exact container ID when the [`FdbContainer`] is dropped, and
//! also if startup fails part-way. Removal errors are logged to stderr and never panic. The
//! crate never removes containers by name pattern, label or filter. If the process is killed
//! (SIGKILL, abort) the container is leaked; it can be found with
//! `docker ps -a --filter label=fdb-native-testkit=1` and removed by ID.
//!
//! # Docker Desktop (macOS / Windows): not implemented
//!
//! On Docker Desktop the container bridge IP is not routable from the host, so the approach
//! above does not work. The documented alternative is to publish port 4500 (for example
//! `-p 4500:4500`), start the container with `-e FDB_NETWORKING_MODE=host` (or
//! `-e FDB_COORDINATOR=<public address>` / a public address in the cluster file) so the server
//! advertises `127.0.0.1:4500` rather than the bridge IP, and write a cluster file pointing at
//! that address. Only the Linux path is implemented today.
//!
//! # Future work
//!
//! A compose-based multi-process cluster variant (several `fdbserver` processes, redundancy
//! modes beyond `single`, fault-injection scenarios) is not implemented yet.

use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

/// Environment variable selecting the Docker image.
pub const IMAGE_ENV: &str = "FDB_IMAGE";

/// Image used when [`IMAGE_ENV`] is unset or empty.
pub const DEFAULT_IMAGE: &str = "foundationdb/foundationdb:7.4.10";

/// Label put on every container created by this crate.
pub const LABEL: &str = "fdb-native-testkit=1";

/// Port the FDB server listens on inside the container.
pub const FDB_PORT: u16 = 4500;

/// Default overall timeout for configuring the database and waiting for readiness.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

const DOCKER: &str = "docker";
const NAME_PREFIX: &str = "fdb-native-test";
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Per-invocation fdbcli timeout, in seconds, so a hung fdbcli cannot outlive the deadline.
const FDBCLI_TIMEOUT_SECS: &str = "5";

/// Errors produced while starting or talking to a disposable FDB container.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The `docker` binary could not be executed.
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
    /// The output of a docker command could not be understood.
    #[error("unexpected docker output: {0}")]
    Parse(String),
    /// The cluster file could not be created.
    #[error("failed to write cluster file: {0}")]
    ClusterFile(#[source] io::Error),
    /// The database did not become available before the deadline.
    #[error(
        "FoundationDB container {container} was not ready within {timeout:?} \
         (last fdbcli output: {last_output:?}; container logs: {logs:?})"
    )]
    NotReady {
        /// The container ID.
        container: String,
        /// The timeout that elapsed.
        timeout: Duration,
        /// The last output seen from fdbcli.
        last_output: String,
        /// The tail of the container logs (best effort).
        logs: String,
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

/// Parses the output of
/// `docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{"\n"}}{{end}}'`,
/// returning the first non-empty address.
fn parse_inspect_ip(output: &str) -> Option<IpAddr> {
    output
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .find_map(|l| l.parse().ok())
}

/// Validates and returns a container ID printed by `docker run -d`.
fn parse_container_id(output: &str) -> Option<String> {
    // Docker may print pull progress or warnings before the ID in some setups; the ID is the
    // last non-empty line.
    let id = output.lines().map(str::trim).rfind(|l| !l.is_empty())?;
    let valid = id.len() >= 12 && id.bytes().all(|b| b.is_ascii_hexdigit());
    valid.then(|| id.to_owned())
}

/// Parses the output of `docker inspect -f '{{.Id}} {{index .Config.Labels "fdb-native-testkit"}}'`
/// and returns the ID if (and only if) the container carries our label.
fn parse_labeled_id(output: &str) -> Option<String> {
    let mut parts = output.split_whitespace();
    let id = parts.next()?;
    let label = parts.next()?;
    (label == "1" && id.bytes().all(|b| b.is_ascii_hexdigit())).then(|| id.to_owned())
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

fn describe(args: &[&str]) -> String {
    args.join(" ")
}

/// Runs `docker <args>` and returns the raw output regardless of exit status.
fn docker_raw(args: &[&str]) -> Result<Output> {
    Command::new(DOCKER)
        .args(args)
        .output()
        .map_err(Error::Spawn)
}

/// Runs `docker <args>`, requiring success, and returns stdout.
fn docker(args: &[&str]) -> Result<String> {
    let out = docker_raw(args)?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(Error::Docker {
            command: describe(args),
            status: out.status.to_string(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        })
    }
}

/// Owns a container ID and force-removes exactly that container on drop.
#[derive(Debug)]
struct ContainerGuard {
    id: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        match docker_raw(&["rm", "-f", &self.id]) {
            Ok(out) if out.status.success() => {}
            Ok(out) => eprintln!(
                "fdb-native-testkit: `docker rm -f {}` failed ({}): {}",
                self.id,
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ),
            Err(e) => eprintln!(
                "fdb-native-testkit: could not run `docker rm -f {}`: {e}",
                self.id
            ),
        }
    }
}

/// `docker run` can fail after creating the container (e.g. the entrypoint cannot start).
/// Resolve our uniquely named container to its ID, verify our label, and remove it by ID.
fn cleanup_failed_run(name: &str) {
    let Ok(out) = docker(&[
        "inspect",
        "-f",
        "{{.Id}} {{index .Config.Labels \"fdb-native-testkit\"}}",
        name,
    ]) else {
        return; // no such container: nothing leaked
    };
    if let Some(id) = parse_labeled_id(&out) {
        drop(ContainerGuard { id });
    }
}

// ---------------------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------------------

/// Builder for [`FdbContainer`].
#[derive(Debug, Clone)]
pub struct FdbContainerBuilder {
    image: String,
    startup_timeout: Duration,
    name: Option<String>,
}

impl Default for FdbContainerBuilder {
    fn default() -> Self {
        Self {
            image: default_image(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            name: None,
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

    /// Sets the overall timeout for configuring the database and waiting for readiness.
    #[must_use]
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Overrides the container name. It must be unique; the default is generated.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Starts the container and waits until the database is available.
    pub fn start(self) -> Result<FdbContainer> {
        let name = self.name.unwrap_or_else(generate_container_name);
        let deadline = Instant::now() + self.startup_timeout;

        let run_out = docker_raw(&["run", "-d", "--name", &name, "--label", LABEL, &self.image])?;
        let stdout = String::from_utf8_lossy(&run_out.stdout);
        let id = if run_out.status.success() {
            parse_container_id(&stdout)
        } else {
            None
        };
        let Some(id) = id else {
            let err = if run_out.status.success() {
                Error::Parse(format!("docker run printed no container id: {stdout:?}"))
            } else {
                Error::Docker {
                    command: format!("run -d --name {name} --label {LABEL} {}", self.image),
                    status: run_out.status.to_string(),
                    stderr: String::from_utf8_lossy(&run_out.stderr).trim().to_owned(),
                }
            };
            cleanup_failed_run(&name);
            return Err(err);
        };

        // From here on the guard guarantees removal, including on early return.
        let guard = ContainerGuard { id };

        let ip_out = docker(&[
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{\"\\n\"}}{{end}}",
            &guard.id,
        ])?;
        let ip = parse_inspect_ip(&ip_out).ok_or_else(|| {
            Error::Parse(format!(
                "no container IP address in inspect output {ip_out:?}"
            ))
        })?;
        let address = SocketAddr::new(ip, FDB_PORT);
        let cluster_string = format_cluster_string(address);

        let dir = tempfile::Builder::new()
            .prefix("fdb-native-testkit-")
            .tempdir()
            .map_err(Error::ClusterFile)?;
        let cluster_file = dir.path().join("fdb.cluster");
        fs::write(&cluster_file, format!("{cluster_string}\n")).map_err(Error::ClusterFile)?;

        let container = FdbContainer {
            guard,
            image: self.image,
            name,
            address,
            cluster_string,
            cluster_file,
            _dir: dir,
        };
        container.wait_ready(self.startup_timeout, deadline)?;
        Ok(container)
    }
}

/// A disposable single-process, in-memory FoundationDB server running in a Docker container.
///
/// The container is removed when this handle is dropped. See the [crate docs](crate).
#[derive(Debug)]
pub struct FdbContainer {
    // Field order matters for drop order: remove the container before the temp dir.
    guard: ContainerGuard,
    image: String,
    name: String,
    address: SocketAddr,
    cluster_string: String,
    cluster_file: PathBuf,
    _dir: TempDir,
}

impl FdbContainer {
    /// Starts a container using [`default_image`] and the default timeout.
    pub fn start() -> Result<Self> {
        Self::builder().start()
    }

    /// Starts a container for `image` with the default timeout.
    pub fn start_with_image(image: impl Into<String>) -> Result<Self> {
        Self::builder().image(image).start()
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

    /// Runs `fdbcli --exec <command>` inside the container and returns its stdout.
    ///
    /// Fails if fdbcli exits non-zero.
    pub fn fdbcli(&self, command: &str) -> Result<String> {
        docker(&[
            "exec",
            &self.guard.id,
            "fdbcli",
            "--timeout",
            FDBCLI_TIMEOUT_SECS,
            "--exec",
            command,
        ])
    }

    /// Runs fdbcli and returns stdout + stderr, regardless of exit status.
    fn fdbcli_lenient(&self, command: &str) -> Result<String> {
        let out = docker_raw(&[
            "exec",
            &self.guard.id,
            "fdbcli",
            "--timeout",
            FDBCLI_TIMEOUT_SECS,
            "--exec",
            command,
        ])?;
        let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
        s.push_str(&String::from_utf8_lossy(&out.stderr));
        Ok(s)
    }

    fn wait_ready(&self, timeout: Duration, deadline: Instant) -> Result<()> {
        let mut last_output = String::new();
        let mut configured = false;
        while Instant::now() < deadline {
            let step = if configured {
                self.fdbcli_lenient("status minimal")
                    .map(|o| (status_is_available(&o), o))
            } else {
                self.fdbcli_lenient("configure new single memory")
                    .map(|o| (configure_succeeded(&o), o))
            };
            match step {
                Ok((true, o)) => {
                    if configured {
                        return Ok(());
                    }
                    configured = true;
                    last_output = o;
                    continue; // go straight to the readiness check
                }
                Ok((false, o)) => last_output = o,
                Err(e) => last_output = e.to_string(),
            }
            thread::sleep(POLL_INTERVAL);
        }
        let logs = docker(&["logs", "--tail", "20", &self.guard.id])
            .unwrap_or_else(|e| format!("<unavailable: {e}>"));
        Err(Error::NotReady {
            container: self.guard.id.clone(),
            timeout,
            last_output: last_output.trim().to_owned(),
            logs,
        })
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
            parse_inspect_ip("172.17.0.2\n"),
            Some(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 2)))
        );
        // Multiple networks, first one empty (e.g. host/none network).
        assert_eq!(
            parse_inspect_ip("\n10.0.0.5\n172.18.0.3\n"),
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)))
        );
        assert_eq!(parse_inspect_ip(""), None);
        assert_eq!(parse_inspect_ip("\n\n"), None);
        assert_eq!(parse_inspect_ip("not-an-ip\n"), None);
    }

    #[test]
    fn container_id_parsing() {
        let id = "a".repeat(64);
        assert_eq!(parse_container_id(&format!("{id}\n")), Some(id.clone()));
        assert_eq!(
            parse_container_id(&format!("WARNING: something\n{id}\n")),
            Some(id)
        );
        assert_eq!(parse_container_id(""), None);
        assert_eq!(parse_container_id("Error response from daemon\n"), None);
        assert_eq!(parse_container_id("abc"), None);
    }

    #[test]
    fn labeled_id_parsing() {
        let id = "b".repeat(64);
        assert_eq!(parse_labeled_id(&format!("{id} 1\n")), Some(id.clone()));
        assert_eq!(parse_labeled_id(&format!("{id} \n")), None);
        assert_eq!(parse_labeled_id(&format!("{id} 0\n")), None);
        assert_eq!(parse_labeled_id(""), None);
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
}
