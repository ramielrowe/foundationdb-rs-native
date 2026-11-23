//! FoundationDB-compatible TLS configuration: options, environment
//! variables, default file locations and PEM loading.
//!
//! [`TlsConfig`] holds what a program sets explicitly (FoundationDB's
//! *network options*, such as `TLS_cert_path`). [`TlsConfig::load`] resolves
//! it against the environment and the default config directory, in
//! FoundationDB's order, and reads any files, producing a
//! [`LoadedTlsConfig`]: PEM bytes plus the resolved password and
//! verify-peers strings.
//!
//! See the [module documentation](super) for the FoundationDB behaviour this
//! mirrors.

use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};

use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use super::error::TlsError;
use super::pem;

/// Environment variable: path of the certificate (chain) PEM file.
pub const ENV_CERTIFICATE_FILE: &str = "FDB_TLS_CERTIFICATE_FILE";
/// Environment variable: path of the private key PEM file.
pub const ENV_KEY_FILE: &str = "FDB_TLS_KEY_FILE";
/// Environment variable: path of the CA bundle PEM file.
pub const ENV_CA_FILE: &str = "FDB_TLS_CA_FILE";
/// Environment variable: passphrase of an encrypted private key.
pub const ENV_PASSWORD: &str = "FDB_TLS_PASSWORD";
/// Environment variable: peer verification rules (`verify_peers` string).
pub const ENV_VERIFY_PEERS: &str = "FDB_TLS_VERIFY_PEERS";
/// Environment variable: `1` or `0`; refuse (or allow) plaintext connections.
pub const ENV_DISABLE_PLAINTEXT_CONNECTION: &str = "FDB_TLS_DISABLE_PLAINTEXT_CONNECTION";

/// The verify-peers rule FoundationDB uses when none is configured.
pub const DEFAULT_VERIFY_PEERS: &str = "Check.Valid=1";

/// File name looked up in the default config directory for the certificate.
pub const DEFAULT_CERTIFICATE_FILE_NAME: &str = "cert.pem";
/// File name looked up in the default config directory for the private key.
pub const DEFAULT_KEY_FILE_NAME: &str = "key.pem";

/// Largest certificate, key or CA file that will be read, in bytes.
///
/// FoundationDB's `CERT_FILE_MAX_SIZE` knob defaults to 5 MiB.
pub const MAX_FILE_SIZE: u64 = 5 * 1024 * 1024;

/// FoundationDB's default configuration directory for this platform
/// (`platform::getDefaultConfigPath`): `/etc/foundationdb` on Linux,
/// `/usr/local/etc/foundationdb` on macOS and FreeBSD,
/// `%ProgramData%\foundationdb` on Windows. `None` on other platforms.
pub fn default_config_dir() -> Option<PathBuf> {
    default_config_dir_impl()
}

#[cfg(target_os = "linux")]
fn default_config_dir_impl() -> Option<PathBuf> {
    Some(PathBuf::from("/etc/foundationdb"))
}

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
fn default_config_dir_impl() -> Option<PathBuf> {
    Some(PathBuf::from("/usr/local/etc/foundationdb"))
}

#[cfg(windows)]
fn default_config_dir_impl() -> Option<PathBuf> {
    let base = std::env::var_os("ProgramData")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"));
    Some(base.join("foundationdb"))
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    windows
)))]
fn default_config_dir_impl() -> Option<PathBuf> {
    None
}

/// Where explicitly configured material comes from.
#[derive(Clone, PartialEq, Eq)]
enum Source {
    Path(PathBuf),
    Bytes(Vec<u8>),
}

/// TLS settings as a program sets them, before the environment and default
/// file locations are consulted.
///
/// The setters mirror FoundationDB's network options:
///
/// | FoundationDB option           | Setter                                   |
/// |-------------------------------|------------------------------------------|
/// | `TLS_cert_path`               | [`cert_path`](Self::cert_path)           |
/// | `TLS_cert_bytes`              | [`cert_bytes`](Self::cert_bytes)         |
/// | `TLS_key_path`                | [`key_path`](Self::key_path)             |
/// | `TLS_key_bytes`               | [`key_bytes`](Self::key_bytes)           |
/// | `TLS_ca_path`                 | [`ca_path`](Self::ca_path)               |
/// | `TLS_ca_bytes`                | [`ca_bytes`](Self::ca_bytes)             |
/// | `TLS_password`                | [`password`](Self::password)             |
/// | `TLS_verify_peers`            | [`verify_peers`](Self::verify_peers)     |
/// | `TLS_disable_plaintext_connection` | [`disable_plaintext_connection`](Self::disable_plaintext_connection) |
///
/// As in FoundationDB, setting a path clears any bytes for the same item and
/// vice versa; the last call wins.
///
/// An empty `TlsConfig` is valid: [`load`](Self::load) then falls back to the
/// `FDB_TLS_*` environment variables and `cert.pem` / `key.pem` in the
/// default config directory.
///
/// # Examples
///
/// ```
/// use fdb_native_transport::tls::TlsConfig;
///
/// let config = TlsConfig::new()
///     .ca_path("/etc/fdb/ca.pem")
///     .cert_path("/etc/fdb/cert.pem")
///     .key_path("/etc/fdb/key.pem")
///     .verify_peers("Check.Valid=1,I.CN=My CA");
/// # let _ = config;
/// ```
#[derive(Clone, Default)]
pub struct TlsConfig {
    cert: Option<Source>,
    key: Option<Source>,
    ca: Option<Source>,
    password: Option<String>,
    verify_peers: Vec<String>,
    disable_plaintext: bool,
}

impl TlsConfig {
    /// A configuration with nothing set explicitly.
    pub fn new() -> Self {
        Self::default()
    }

    /// A configuration holding the `FDB_TLS_*` variables of the current
    /// process as explicit settings.
    ///
    /// This is a snapshot: later changes to the environment are not seen.
    /// It is rarely needed, since [`load`](Self::load) already consults the
    /// environment. See [`from_env_with`](Self::from_env_with).
    pub fn from_env() -> Self {
        Self::from_env_with(|name| std::env::var(name).ok())
    }

    /// Like [`from_env`](Self::from_env) with an injected variable lookup,
    /// so callers and tests need not touch the process environment.
    ///
    /// A variable that is set but empty counts as unset here.
    pub fn from_env_with(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let get = |name: &str| lookup(name).filter(|v| !v.is_empty());
        let mut config = Self::new();
        if let Some(path) = get(ENV_CERTIFICATE_FILE) {
            config = config.cert_path(path);
        }
        if let Some(path) = get(ENV_KEY_FILE) {
            config = config.key_path(path);
        }
        if let Some(path) = get(ENV_CA_FILE) {
            config = config.ca_path(path);
        }
        if let Some(password) = get(ENV_PASSWORD) {
            config = config.password(password);
        }
        if let Some(rules) = get(ENV_VERIFY_PEERS) {
            config = config.verify_peers(rules);
        }
        if let Some(flag) = lookup(ENV_DISABLE_PLAINTEXT_CONNECTION).and_then(parse_bool) {
            config = config.disable_plaintext_connection(flag);
        }
        config
    }

    /// Reads the certificate chain from a PEM file (`TLS_cert_path`).
    ///
    /// The first certificate is the client's own; the rest are sent as its
    /// chain.
    #[must_use]
    pub fn cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.cert = Some(Source::Path(path.into()));
        self
    }

    /// Uses PEM bytes as the certificate chain (`TLS_cert_bytes`).
    #[must_use]
    pub fn cert_bytes(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.cert = Some(Source::Bytes(pem.into()));
        self
    }

    /// Reads the private key from a PEM file (`TLS_key_path`).
    #[must_use]
    pub fn key_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.key = Some(Source::Path(path.into()));
        self
    }

    /// Uses PEM bytes as the private key (`TLS_key_bytes`).
    #[must_use]
    pub fn key_bytes(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.key = Some(Source::Bytes(pem.into()));
        self
    }

    /// Reads the CA bundle (the trust anchors) from a PEM file
    /// (`TLS_ca_path`).
    #[must_use]
    pub fn ca_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca = Some(Source::Path(path.into()));
        self
    }

    /// Uses PEM bytes as the CA bundle (`TLS_ca_bytes`).
    #[must_use]
    pub fn ca_bytes(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca = Some(Source::Bytes(pem.into()));
        self
    }

    /// Sets the passphrase for an encrypted private key (`TLS_password`).
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Sets the peer verification rules (`TLS_verify_peers`), replacing any
    /// set before. Rules are separated by `|`; see the verify-peers
    /// documentation. This crate stores the string; interpreting it is the
    /// verifier's job.
    #[must_use]
    pub fn verify_peers(mut self, rules: impl Into<String>) -> Self {
        self.verify_peers.clear();
        self.verify_peers.push(rules.into());
        self
    }

    /// Appends one more verify-peers rule string (FoundationDB's repeated
    /// `--tls-verify-peers` command-line flag).
    #[must_use]
    pub fn add_verify_peers(mut self, rules: impl Into<String>) -> Self {
        self.verify_peers.push(rules.into());
        self
    }

    /// Refuses to connect to non-TLS endpoints
    /// (`TLS_disable_plaintext_connection`). The flag is carried through
    /// to [`LoadedTlsConfig::disable_plaintext_connection`]; the transport
    /// enforces it.
    #[must_use]
    pub fn disable_plaintext_connection(mut self, disable: bool) -> Self {
        self.disable_plaintext = disable;
        self
    }

    /// Resolves this configuration against the process environment and the
    /// platform's default config directory, and reads any files.
    ///
    /// This does blocking file I/O. Call it at start-up, or from a blocking
    /// task.
    pub fn load(&self) -> Result<LoadedTlsConfig, TlsError> {
        self.load_with(
            |name| std::env::var(name).ok(),
            default_config_dir().as_deref(),
        )
    }

    /// [`load`](Self::load) with an injected environment lookup and default
    /// config directory (`None` disables the `cert.pem` / `key.pem`
    /// fallback).
    ///
    /// Resolution follows FoundationDB (`TLSConfig::getCertificatePathSync`
    /// and friends, `TLSConfig::loadSync`):
    ///
    /// 1. an explicitly set *path*;
    /// 2. the environment variable (when set, even to the empty string, which
    ///    means "no file" and stops the search);
    /// 3. for the certificate and key only, `cert.pem` / `key.pem` in
    ///    `default_dir`, if the file exists.
    ///
    /// If that yields a path, the file is read. Otherwise explicitly set
    /// *bytes* are used. Note the consequence, which is FoundationDB's: bytes
    /// have the lowest priority, so an environment variable or default file
    /// overrides `cert_bytes` / `key_bytes` / `ca_bytes`.
    ///
    /// The password is the explicit one, else `FDB_TLS_PASSWORD`. Verify
    /// peers is the explicit list, else `FDB_TLS_VERIFY_PEERS`, else
    /// `Check.Valid=1`. The plaintext flag is `FDB_TLS_DISABLE_PLAINTEXT_CONNECTION`
    /// when it is `0` or `1` (any other value is ignored), else the explicit
    /// setting.
    pub fn load_with(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
        default_dir: Option<&Path>,
    ) -> Result<LoadedTlsConfig, TlsError> {
        let cert = resolve_material(
            "certificate",
            self.cert.as_ref(),
            &lookup(ENV_CERTIFICATE_FILE),
            default_dir.map(|d| d.join(DEFAULT_CERTIFICATE_FILE_NAME)),
        )?;
        let key = resolve_material(
            "key",
            self.key.as_ref(),
            &lookup(ENV_KEY_FILE),
            default_dir.map(|d| d.join(DEFAULT_KEY_FILE_NAME)),
        )?;
        let ca = resolve_material("CA", self.ca.as_ref(), &lookup(ENV_CA_FILE), None)?;

        let password = self
            .password
            .clone()
            .filter(|p| !p.is_empty())
            .or_else(|| lookup(ENV_PASSWORD).filter(|p| !p.is_empty()));

        let verify_peers = if self.verify_peers.is_empty() {
            match lookup(ENV_VERIFY_PEERS) {
                Some(rules) => vec![rules],
                None => vec![DEFAULT_VERIFY_PEERS.to_owned()],
            }
        } else {
            self.verify_peers.clone()
        };

        let disable_plaintext_connection = lookup(ENV_DISABLE_PLAINTEXT_CONNECTION)
            .and_then(parse_bool)
            .unwrap_or(self.disable_plaintext);

        Ok(LoadedTlsConfig {
            cert,
            key,
            ca,
            password,
            verify_peers,
            disable_plaintext_connection,
        })
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig")
            .field("cert", &self.cert)
            .field("key", &self.key)
            .field("ca", &self.ca)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("verify_peers", &self.verify_peers)
            .field("disable_plaintext_connection", &self.disable_plaintext)
            .finish()
    }
}

impl fmt::Debug for Source {
    /// Shows the path, or only the byte length: bytes may be a private key.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Path(path) => write!(f, "Path({})", path.display()),
            Source::Bytes(bytes) => write!(f, "Bytes({} bytes)", bytes.len()),
        }
    }
}

/// `boost::lexical_cast<bool>`: only `0` and `1`.
fn parse_bool(value: String) -> Option<bool> {
    match value.as_str() {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

fn resolve_material(
    what: &'static str,
    explicit: Option<&Source>,
    env_path: &Option<String>,
    default_file: Option<PathBuf>,
) -> Result<Option<Vec<u8>>, TlsError> {
    let explicit_path = match explicit {
        Some(Source::Path(path)) if !path.as_os_str().is_empty() => Some(path.clone()),
        _ => None,
    };
    let path = match (explicit_path, env_path) {
        (Some(path), _) => Some(path),
        (None, Some(env)) => (!env.is_empty()).then(|| PathBuf::from(env)),
        (None, None) => default_file.filter(|file| file.is_file()),
    };
    if let Some(path) = path {
        return read_limited(what, path).map(Some);
    }
    Ok(match explicit {
        Some(Source::Bytes(bytes)) if !bytes.is_empty() => Some(bytes.clone()),
        _ => None,
    })
}

fn read_limited(what: &'static str, path: PathBuf) -> Result<Vec<u8>, TlsError> {
    let read = |path: &Path| -> std::io::Result<Vec<u8>> {
        let mut data = Vec::new();
        std::fs::File::open(path)?
            .take(MAX_FILE_SIZE + 1)
            .read_to_end(&mut data)?;
        Ok(data)
    };
    let data = read(&path).map_err(|source| TlsError::ReadFile {
        what,
        path: path.clone(),
        source,
    })?;
    if data.len() as u64 > MAX_FILE_SIZE {
        return Err(TlsError::FileTooLarge {
            what,
            path,
            limit: MAX_FILE_SIZE,
        });
    }
    Ok(data)
}

/// TLS material and settings after [`TlsConfig::load`]: PEM bytes read from
/// their files, and the password and verify-peers strings resolved.
///
/// The accessors return raw PEM; the parsing helpers
/// ([`ca_certificates`](Self::ca_certificates),
/// [`client_identity`](Self::client_identity)) decode it.
#[derive(Clone, PartialEq, Eq)]
pub struct LoadedTlsConfig {
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    ca: Option<Vec<u8>>,
    password: Option<String>,
    verify_peers: Vec<String>,
    disable_plaintext_connection: bool,
}

impl LoadedTlsConfig {
    /// PEM of the client certificate chain, if one is configured.
    pub fn certificate_pem(&self) -> Option<&[u8]> {
        self.cert.as_deref()
    }

    /// PEM of the client private key, if one is configured.
    pub fn key_pem(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// PEM of the CA bundle, if one is configured.
    pub fn ca_pem(&self) -> Option<&[u8]> {
        self.ca.as_deref()
    }

    /// The passphrase for an encrypted key, if one is configured.
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// The resolved verify-peers rule strings: the explicit ones, else
    /// `FDB_TLS_VERIFY_PEERS`, else `["Check.Valid=1"]`. Never empty.
    pub fn verify_peers(&self) -> &[String] {
        &self.verify_peers
    }

    /// Whether plaintext connections must be refused.
    pub fn disable_plaintext_connection(&self) -> bool {
        self.disable_plaintext_connection
    }

    /// The CA bundle decoded to DER certificates.
    ///
    /// Errors with [`TlsError::NoTrustAnchors`] if no CA is configured.
    pub fn ca_certificates(&self) -> Result<Vec<CertificateDer<'static>>, TlsError> {
        match self.ca_pem() {
            Some(pem) => pem::certificates("CA", pem),
            None => Err(TlsError::NoTrustAnchors),
        }
    }

    /// The client certificate chain and private key, decoded and (if the key
    /// is encrypted) decrypted.
    ///
    /// Returns `Ok(None)` when neither a certificate nor a key is configured:
    /// the connection then presents no client certificate. A certificate
    /// without a key, or a key without a certificate, is an error.
    #[allow(clippy::type_complexity)]
    pub fn client_identity(
        &self,
    ) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>, TlsError> {
        match (self.certificate_pem(), self.key_pem()) {
            (None, None) => Ok(None),
            (Some(_), None) => Err(TlsError::CertificateWithoutKey),
            (None, Some(_)) => Err(TlsError::KeyWithoutCertificate),
            (Some(cert), Some(key)) => {
                let chain = pem::certificates("certificate", cert)?;
                let key = pem::private_key(key, self.password())?;
                Ok(Some((chain, key)))
            }
        }
    }
}

impl fmt::Debug for LoadedTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = |m: &Option<Vec<u8>>| m.as_ref().map(|b| format!("{} bytes", b.len()));
        f.debug_struct("LoadedTlsConfig")
            .field("certificate", &len(&self.cert))
            .field("key", &len(&self.key).map(|_| "<redacted>"))
            .field("ca", &len(&self.ca))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("verify_peers", &self.verify_peers)
            .field(
                "disable_plaintext_connection",
                &self.disable_plaintext_connection,
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    /// A scratch directory under the workspace `target/` (git-ignored),
    /// unique per test.
    fn scratch(name: &str) -> PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/tls-config-tests")
            .join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn empty_config_has_nothing_and_default_rules() {
        let loaded = TlsConfig::new().load_with(no_env, None).unwrap();
        assert!(loaded.certificate_pem().is_none());
        assert!(loaded.key_pem().is_none());
        assert!(loaded.ca_pem().is_none());
        assert!(loaded.password().is_none());
        assert_eq!(loaded.verify_peers(), ["Check.Valid=1"]);
        assert!(!loaded.disable_plaintext_connection());
        assert!(loaded.client_identity().unwrap().is_none());
        assert!(matches!(
            loaded.ca_certificates(),
            Err(TlsError::NoTrustAnchors)
        ));
    }

    #[test]
    fn explicit_bytes_are_used_when_nothing_overrides_them() {
        let loaded = TlsConfig::new()
            .cert_bytes(&b"CERT"[..])
            .key_bytes(&b"KEY"[..])
            .ca_bytes(&b"CA"[..])
            .password("pw")
            .load_with(no_env, None)
            .unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"CERT"[..]));
        assert_eq!(loaded.key_pem(), Some(&b"KEY"[..]));
        assert_eq!(loaded.ca_pem(), Some(&b"CA"[..]));
        assert_eq!(loaded.password(), Some("pw"));
    }

    #[test]
    fn empty_bytes_count_as_unset() {
        let loaded = TlsConfig::new()
            .cert_bytes(Vec::new())
            .load_with(no_env, None)
            .unwrap();
        assert!(loaded.certificate_pem().is_none());
    }

    #[test]
    fn explicit_path_beats_env_and_default_dir() {
        let dir = scratch("path-precedence");
        let explicit = dir.join("explicit.pem");
        let from_env = dir.join("env.pem");
        std::fs::write(&explicit, b"EXPLICIT").unwrap();
        std::fs::write(&from_env, b"ENV").unwrap();
        std::fs::write(dir.join("cert.pem"), b"DEFAULT").unwrap();

        let vars = [(ENV_CERTIFICATE_FILE, from_env.to_str().unwrap())];
        let loaded = TlsConfig::new()
            .cert_path(&explicit)
            .load_with(env(&vars), Some(&dir))
            .unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"EXPLICIT"[..]));

        let loaded = TlsConfig::new().load_with(env(&vars), Some(&dir)).unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"ENV"[..]));

        let loaded = TlsConfig::new().load_with(no_env, Some(&dir)).unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"DEFAULT"[..]));
    }

    #[test]
    fn default_dir_supplies_cert_and_key_but_not_ca() {
        let dir = scratch("default-dir");
        std::fs::write(dir.join("cert.pem"), b"C").unwrap();
        std::fs::write(dir.join("key.pem"), b"K").unwrap();
        std::fs::write(dir.join("ca.pem"), b"A").unwrap();
        let loaded = TlsConfig::new().load_with(no_env, Some(&dir)).unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"C"[..]));
        assert_eq!(loaded.key_pem(), Some(&b"K"[..]));
        assert!(loaded.ca_pem().is_none());
    }

    #[test]
    fn env_and_default_file_override_explicit_bytes() {
        // FoundationDB quirk: getCertificatePathSync() is consulted before
        // the configured bytes are used.
        let dir = scratch("bytes-lowest");
        std::fs::write(dir.join("cert.pem"), b"DEFAULT").unwrap();
        let loaded = TlsConfig::new()
            .cert_bytes(&b"BYTES"[..])
            .load_with(no_env, Some(&dir))
            .unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"DEFAULT"[..]));

        let file = dir.join("ca.pem");
        std::fs::write(&file, b"FROM-ENV").unwrap();
        let vars = [(ENV_CA_FILE, file.to_str().unwrap())];
        let loaded = TlsConfig::new()
            .ca_bytes(&b"BYTES"[..])
            .load_with(env(&vars), None)
            .unwrap();
        assert_eq!(loaded.ca_pem(), Some(&b"FROM-ENV"[..]));
    }

    #[test]
    fn empty_env_var_stops_the_search_and_falls_back_to_bytes() {
        let dir = scratch("empty-env");
        std::fs::write(dir.join("cert.pem"), b"DEFAULT").unwrap();
        let loaded = TlsConfig::new()
            .cert_bytes(&b"BYTES"[..])
            .load_with(env(&[(ENV_CERTIFICATE_FILE, "")]), Some(&dir))
            .unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"BYTES"[..]));
    }

    #[test]
    fn setting_a_path_clears_bytes_and_vice_versa() {
        let dir = scratch("path-vs-bytes");
        let file = dir.join("c.pem");
        std::fs::write(&file, b"FILE").unwrap();
        let loaded = TlsConfig::new()
            .cert_bytes(&b"BYTES"[..])
            .cert_path(&file)
            .load_with(no_env, None)
            .unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"FILE"[..]));

        let loaded = TlsConfig::new()
            .cert_path(&file)
            .cert_bytes(&b"BYTES"[..])
            .load_with(no_env, None)
            .unwrap();
        assert_eq!(loaded.certificate_pem(), Some(&b"BYTES"[..]));
    }

    #[test]
    fn missing_file_is_an_error_naming_the_path() {
        let err = TlsConfig::new()
            .ca_path("/nonexistent/fdb/ca.pem")
            .load_with(no_env, None)
            .unwrap_err();
        match err {
            TlsError::ReadFile { what, path, .. } => {
                assert_eq!(what, "CA");
                assert_eq!(path, Path::new("/nonexistent/fdb/ca.pem"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn oversized_file_is_rejected() {
        let dir = scratch("oversized");
        let file = dir.join("big.pem");
        std::fs::write(&file, vec![b'x'; MAX_FILE_SIZE as usize + 1]).unwrap();
        let err = TlsConfig::new()
            .ca_path(&file)
            .load_with(no_env, None)
            .unwrap_err();
        assert!(matches!(err, TlsError::FileTooLarge { what: "CA", .. }));

        std::fs::write(&file, vec![b'x'; MAX_FILE_SIZE as usize]).unwrap();
        assert!(
            TlsConfig::new()
                .ca_path(&file)
                .load_with(no_env, None)
                .is_ok()
        );
    }

    #[test]
    fn password_precedence() {
        let vars = [(ENV_PASSWORD, "from-env")];
        let loaded = TlsConfig::new().load_with(env(&vars), None).unwrap();
        assert_eq!(loaded.password(), Some("from-env"));
        let loaded = TlsConfig::new()
            .password("explicit")
            .load_with(env(&vars), None)
            .unwrap();
        assert_eq!(loaded.password(), Some("explicit"));
        let loaded = TlsConfig::new()
            .password("")
            .load_with(env(&vars), None)
            .unwrap();
        assert_eq!(loaded.password(), Some("from-env"));
    }

    #[test]
    fn verify_peers_precedence_and_replacement() {
        let vars = [(ENV_VERIFY_PEERS, "Check.Unexpired=0")];
        let loaded = TlsConfig::new().load_with(env(&vars), None).unwrap();
        assert_eq!(loaded.verify_peers(), ["Check.Unexpired=0"]);

        let loaded = TlsConfig::new()
            .verify_peers("S.CN=a")
            .verify_peers("S.CN=b")
            .load_with(env(&vars), None)
            .unwrap();
        assert_eq!(loaded.verify_peers(), ["S.CN=b"]);

        let loaded = TlsConfig::new()
            .verify_peers("S.CN=a")
            .add_verify_peers("S.CN=b")
            .load_with(no_env, None)
            .unwrap();
        assert_eq!(loaded.verify_peers(), ["S.CN=a", "S.CN=b"]);
    }

    #[test]
    fn plaintext_flag_env_overrides_the_option_when_valid() {
        let on = TlsConfig::new().disable_plaintext_connection(true);
        let off = TlsConfig::new();
        let load = |config: &TlsConfig, value: &str| {
            config
                .load_with(env(&[(ENV_DISABLE_PLAINTEXT_CONNECTION, value)]), None)
                .unwrap()
                .disable_plaintext_connection()
        };
        assert!(load(&off, "1"));
        assert!(!load(&on, "0"));
        // Not a boost-style bool: ignored, the explicit option stands.
        assert!(load(&on, "true"));
        assert!(!load(&off, "yes"));
        assert!(
            on.load_with(no_env, None)
                .unwrap()
                .disable_plaintext_connection()
        );
    }

    #[test]
    fn from_env_snapshots_the_variables() {
        let config = TlsConfig::from_env_with(env(&[
            (ENV_CERTIFICATE_FILE, "/c.pem"),
            (ENV_KEY_FILE, "/k.pem"),
            (ENV_CA_FILE, "/ca.pem"),
            (ENV_PASSWORD, "pw"),
            (ENV_VERIFY_PEERS, "S.O=x"),
            (ENV_DISABLE_PLAINTEXT_CONNECTION, "1"),
        ]));
        assert_eq!(config.cert, Some(Source::Path("/c.pem".into())));
        assert_eq!(config.key, Some(Source::Path("/k.pem".into())));
        assert_eq!(config.ca, Some(Source::Path("/ca.pem".into())));
        assert_eq!(config.password.as_deref(), Some("pw"));
        assert_eq!(config.verify_peers, ["S.O=x"]);
        assert!(config.disable_plaintext);

        let empty = TlsConfig::from_env_with(env(&[(ENV_CA_FILE, "")]));
        assert_eq!(empty.ca, None);
    }

    #[test]
    fn certificate_without_key_is_an_error() {
        let loaded = TlsConfig::new()
            .cert_bytes(&b"C"[..])
            .load_with(no_env, None)
            .unwrap();
        assert!(matches!(
            loaded.client_identity(),
            Err(TlsError::CertificateWithoutKey)
        ));
        let loaded = TlsConfig::new()
            .key_bytes(&b"K"[..])
            .load_with(no_env, None)
            .unwrap();
        assert!(matches!(
            loaded.client_identity(),
            Err(TlsError::KeyWithoutCertificate)
        ));
    }

    #[test]
    fn debug_output_hides_secrets() {
        let config = TlsConfig::new()
            .password("s3cret")
            .key_bytes(&b"PRIVATE"[..]);
        let text = format!("{config:?}");
        assert!(!text.contains("s3cret"), "{text}");
        assert!(!text.contains("PRIVATE"), "{text}");
        let loaded = config.load_with(no_env, None).unwrap();
        let text = format!("{loaded:?}");
        assert!(!text.contains("s3cret"), "{text}");
        assert!(!text.contains("PRIVATE"), "{text}");
    }

    #[test]
    fn default_dir_is_foundationdb_etc_on_linux() {
        if cfg!(target_os = "linux") {
            assert_eq!(
                default_config_dir(),
                Some(PathBuf::from("/etc/foundationdb"))
            );
        }
    }
}
