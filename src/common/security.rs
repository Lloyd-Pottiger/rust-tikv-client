// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use log::info;
use regex::Regex;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Identity;
use tonic::transport::{Certificate, Endpoint};

use crate::internal_err;
use crate::Result;

lazy_static::lazy_static! {
    static ref SCHEME_REG: Regex = {
        #[allow(clippy::expect_used)]
        let reg = Regex::new(r"^\s*(https?://)").expect("invalid scheme regex");
        reg
    };
}

fn check_pem_file(tag: &str, path: &Path) -> Result<File> {
    File::open(path)
        .map_err(|e| internal_err!("failed to open {} to load {}: {:?}", path.display(), tag, e))
}

fn load_pem_file(tag: &str, path: &Path) -> Result<Vec<u8>> {
    let mut file = check_pem_file(tag, path)?;
    let mut key = vec![];
    file.read_to_end(&mut key)
        .map_err(|e| {
            internal_err!(
                "failed to load {} from path {}: {:?}",
                tag,
                path.display(),
                e
            )
        })
        .map(|_| key)
}

/// Manages the TLS protocol
pub struct SecurityManager {
    /// The PEM encoding of the server’s CA certificates.
    ca: Vec<u8>,
    /// The PEM encoding of the server’s certificate chain.
    cert: Vec<u8>,
    /// The path to the file that contains the PEM encoding of the server’s private key.
    key: PathBuf,
    grpc_keepalive_time: Duration,
    grpc_keepalive_timeout: Duration,
    grpc_initial_window_size: u32,
    grpc_initial_conn_window_size: u32,
}

const DEFAULT_GRPC_KEEPALIVE_TIME: Duration = Duration::from_secs(10);
const DEFAULT_GRPC_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_GRPC_INITIAL_WINDOW_SIZE: u32 = 1 << 27; // 128MiB
const DEFAULT_GRPC_INITIAL_CONN_WINDOW_SIZE: u32 = 1 << 27; // 128MiB

impl Default for SecurityManager {
    fn default() -> Self {
        SecurityManager {
            ca: Vec::new(),
            cert: Vec::new(),
            key: PathBuf::new(),
            grpc_keepalive_time: DEFAULT_GRPC_KEEPALIVE_TIME,
            grpc_keepalive_timeout: DEFAULT_GRPC_KEEPALIVE_TIMEOUT,
            grpc_initial_window_size: DEFAULT_GRPC_INITIAL_WINDOW_SIZE,
            grpc_initial_conn_window_size: DEFAULT_GRPC_INITIAL_CONN_WINDOW_SIZE,
        }
    }
}

impl SecurityManager {
    /// Load TLS configuration from files.
    pub fn load(
        ca_path: impl AsRef<Path>,
        cert_path: impl AsRef<Path>,
        key_path: impl Into<PathBuf>,
    ) -> Result<SecurityManager> {
        let key_path = key_path.into();
        check_pem_file("private key", &key_path)?;
        Ok(SecurityManager {
            ca: load_pem_file("ca", ca_path.as_ref())?,
            cert: load_pem_file("certificate", cert_path.as_ref())?,
            key: key_path,
            grpc_keepalive_time: DEFAULT_GRPC_KEEPALIVE_TIME,
            grpc_keepalive_timeout: DEFAULT_GRPC_KEEPALIVE_TIMEOUT,
            grpc_initial_window_size: DEFAULT_GRPC_INITIAL_WINDOW_SIZE,
            grpc_initial_conn_window_size: DEFAULT_GRPC_INITIAL_CONN_WINDOW_SIZE,
        })
    }

    pub(crate) fn with_grpc_keepalive(mut self, time: Duration, timeout: Duration) -> Self {
        self.grpc_keepalive_time = time;
        self.grpc_keepalive_timeout = timeout;
        self
    }

    pub(crate) fn with_grpc_initial_window_sizes(mut self, stream: u32, connection: u32) -> Self {
        self.grpc_initial_window_size = stream;
        self.grpc_initial_conn_window_size = connection;
        self
    }

    #[cfg(test)]
    pub(crate) fn grpc_keepalive_config(&self) -> (Duration, Duration) {
        (self.grpc_keepalive_time, self.grpc_keepalive_timeout)
    }

    #[cfg(test)]
    pub(crate) fn grpc_window_sizes(&self) -> (u32, u32) {
        (
            self.grpc_initial_window_size,
            self.grpc_initial_conn_window_size,
        )
    }

    /// Connect to gRPC server using TLS connection. If TLS is not configured, use normal connection.
    pub async fn connect<Factory, Client>(
        &self,
        // env: Arc<Environment>,
        addr: &str,
        factory: Factory,
    ) -> Result<Client>
    where
        Factory: FnOnce(Channel) -> Client,
    {
        info!("connect to rpc server at endpoint: {:?}", addr);
        let channel = if !self.ca.is_empty() {
            self.tls_channel(addr).await?
        } else {
            self.default_channel(addr).await?
        };
        let ch = channel.connect().await?;

        Ok(factory(ch))
    }

    async fn tls_channel(&self, addr: &str) -> Result<Endpoint> {
        let addr = "https://".to_string() + &SCHEME_REG.replace(addr, "");
        let builder = self.endpoint(addr.to_string())?;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&self.ca))
            .identity(Identity::from_pem(
                &self.cert,
                load_pem_file("private key", &self.key)?,
            ));
        let builder = builder.tls_config(tls)?;
        Ok(builder)
    }

    async fn default_channel(&self, addr: &str) -> Result<Endpoint> {
        let addr = "http://".to_string() + &SCHEME_REG.replace(addr, "");
        self.endpoint(addr)
    }

    fn endpoint(&self, addr: String) -> Result<Endpoint> {
        let mut endpoint = Channel::from_shared(addr)?
            .tcp_keepalive(Some(Duration::from_secs(10)))
            .initial_stream_window_size(
                (self.grpc_initial_window_size != 0).then_some(self.grpc_initial_window_size),
            )
            .initial_connection_window_size(
                (self.grpc_initial_conn_window_size != 0)
                    .then_some(self.grpc_initial_conn_window_size),
            )
            .keep_alive_while_idle(false);

        if self.grpc_keepalive_time != Duration::ZERO {
            endpoint = endpoint
                .http2_keep_alive_interval(self.grpc_keepalive_time)
                .keep_alive_timeout(self.grpc_keepalive_timeout);
        }

        Ok(endpoint)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    use tempfile;

    use super::*;

    #[test]
    fn test_security_manager_grpc_keepalive_defaults() {
        let mgr = SecurityManager::default();
        assert_eq!(
            mgr.grpc_keepalive_config(),
            (Duration::from_secs(10), Duration::from_secs(3))
        );
        assert_eq!(mgr.grpc_window_sizes(), (1 << 27, 1 << 27));
    }

    #[test]
    fn test_security() {
        let temp = tempfile::tempdir().unwrap();
        let example_ca = temp.path().join("ca");
        let example_cert = temp.path().join("cert");
        let example_pem = temp.path().join("key");
        for (id, f) in [&example_ca, &example_cert, &example_pem]
            .iter()
            .enumerate()
        {
            File::create(f).unwrap().write_all(&[id as u8]).unwrap();
        }
        let cert_path: PathBuf = format!("{}", example_cert.display()).into();
        let key_path: PathBuf = format!("{}", example_pem.display()).into();
        let ca_path: PathBuf = format!("{}", example_ca.display()).into();
        let mgr = SecurityManager::load(ca_path, cert_path, &key_path).unwrap();
        assert_eq!(mgr.ca, vec![0]);
        assert_eq!(mgr.cert, vec![1]);
        let key = load_pem_file("private key", &key_path).unwrap();
        assert_eq!(key, vec![2]);
    }

    #[test]
    fn test_security_manager_with_grpc_keepalive_overrides() {
        let mgr = SecurityManager::default()
            .with_grpc_keepalive(Duration::from_secs(1), Duration::from_secs(2));
        assert_eq!(
            mgr.grpc_keepalive_config(),
            (Duration::from_secs(1), Duration::from_secs(2))
        );
    }

    #[test]
    fn test_security_manager_with_grpc_window_sizes_overrides() {
        let mgr = SecurityManager::default().with_grpc_initial_window_sizes(1, 2);
        assert_eq!(mgr.grpc_window_sizes(), (1, 2));
    }
}
