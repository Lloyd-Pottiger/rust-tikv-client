// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use log::info;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Identity;
use tonic::transport::{Certificate, Endpoint};

use crate::internal_err;
use crate::Error;
use crate::Result;

fn strip_http_scheme(addr: &str) -> &str {
    let addr = addr.trim_start();
    if let Some(stripped) = addr.strip_prefix("http://") {
        return stripped;
    }
    if let Some(stripped) = addr.strip_prefix("https://") {
        return stripped;
    }
    addr
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
    keep_alive_timeout: Duration,
}

const DEFAULT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(3);
const MIN_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_millis(50);

impl Default for SecurityManager {
    fn default() -> Self {
        Self {
            ca: Vec::new(),
            cert: Vec::new(),
            key: PathBuf::new(),
            keep_alive_timeout: DEFAULT_KEEP_ALIVE_TIMEOUT,
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
            keep_alive_timeout: DEFAULT_KEEP_ALIVE_TIMEOUT,
        })
    }

    /// Validate user-configurable settings.
    pub fn validate(&self) -> Result<()> {
        if self.keep_alive_timeout < MIN_KEEP_ALIVE_TIMEOUT {
            return Err(Error::StringError(format!(
                "grpc-keepalive-timeout should be at least {MIN_KEEP_ALIVE_TIMEOUT:?}, but got {:?}",
                self.keep_alive_timeout
            )));
        }
        Ok(())
    }

    /// Get the gRPC keepalive timeout.
    pub fn grpc_keepalive_timeout(&self) -> Duration {
        self.keep_alive_timeout
    }

    /// Set the gRPC keepalive timeout.
    pub fn set_grpc_keepalive_timeout(&mut self, timeout: Duration) {
        self.keep_alive_timeout = timeout;
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
        self.validate()?;
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
        let addr = format!("https://{}", strip_http_scheme(addr));
        let builder = self.endpoint(addr)?;
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
        let addr = format!("http://{}", strip_http_scheme(addr));
        self.endpoint(addr)
    }

    fn endpoint(&self, addr: String) -> Result<Endpoint> {
        let endpoint = Channel::from_shared(addr)?
            .tcp_keepalive(Some(Duration::from_secs(10)))
            .keep_alive_timeout(self.keep_alive_timeout);
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
    fn strip_http_scheme_accepts_plain_and_prefixed_addrs() {
        assert_eq!(super::strip_http_scheme("127.0.0.1:2379"), "127.0.0.1:2379");
        assert_eq!(
            super::strip_http_scheme("http://127.0.0.1:2379"),
            "127.0.0.1:2379"
        );
        assert_eq!(
            super::strip_http_scheme("https://127.0.0.1:2379"),
            "127.0.0.1:2379"
        );
        assert_eq!(
            super::strip_http_scheme("   https://127.0.0.1:2379"),
            "127.0.0.1:2379"
        );
        assert_eq!(
            super::strip_http_scheme("   127.0.0.1:2379"),
            "127.0.0.1:2379"
        );
    }

    #[test]
    fn grpc_keepalive_timeout_matches_client_go_test() {
        let mut mgr = SecurityManager::default();
        assert!(mgr.validate().is_ok());
        assert_eq!(mgr.grpc_keepalive_timeout(), Duration::from_secs(3));

        mgr.set_grpc_keepalive_timeout(Duration::from_millis(50));
        assert!(mgr.validate().is_ok());
        assert_eq!(mgr.grpc_keepalive_timeout(), Duration::from_millis(50));

        mgr.set_grpc_keepalive_timeout(Duration::from_millis(40));
        let err = mgr
            .validate()
            .expect_err("keepalive timeout below 50ms should be invalid");
        assert!(err
            .to_string()
            .contains("grpc-keepalive-timeout should be at least"));
    }
}
