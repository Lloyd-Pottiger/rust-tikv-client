// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::future::Future;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hickory_resolver::config::NameServerConfigGroup;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::TokioAsyncResolver;
use hyper::client::connect::dns::Name;
use hyper::client::connect::HttpConnector;
use log::info;
use regex::Regex;
use tonic::codegen::Service;
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
    grpc_connect_timeout: Duration,
    grpc_custom_dns_server: Option<SocketAddr>,
    grpc_custom_dns_domain: Option<String>,
}

const DEFAULT_GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
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
            grpc_connect_timeout: DEFAULT_GRPC_CONNECT_TIMEOUT,
            grpc_custom_dns_server: None,
            grpc_custom_dns_domain: None,
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
            grpc_connect_timeout: DEFAULT_GRPC_CONNECT_TIMEOUT,
            grpc_custom_dns_server: None,
            grpc_custom_dns_domain: None,
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

    pub(crate) fn with_grpc_connect_timeout(mut self, timeout: Duration) -> Self {
        self.grpc_connect_timeout = timeout;
        self
    }

    pub(crate) fn with_grpc_custom_dns(
        mut self,
        dns_server: Option<SocketAddr>,
        dns_domain: Option<String>,
    ) -> Self {
        self.grpc_custom_dns_server = dns_server;
        self.grpc_custom_dns_domain = dns_domain.filter(|d| !d.is_empty());
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

    #[cfg(test)]
    pub(crate) fn grpc_connect_timeout(&self) -> Duration {
        self.grpc_connect_timeout
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
        let dial_addr = self.dial_addr(addr)?;
        info!("connect to rpc server at endpoint: {:?}", dial_addr);

        let endpoint = if !self.ca.is_empty() {
            self.tls_channel(&dial_addr).await?
        } else {
            self.default_channel(&dial_addr).await?
        };

        let ch = match self.grpc_custom_dns_server {
            Some(dns_server) => {
                let connector = new_custom_dns_connector(dns_server);
                endpoint.connect_with_connector(connector).await?
            }
            None => endpoint.connect().await?,
        };

        Ok(factory(ch))
    }

    async fn tls_channel(&self, addr: &str) -> Result<Endpoint> {
        let addr = "https://".to_string() + addr;
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
        let addr = "http://".to_string() + addr;
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

        if self.grpc_connect_timeout != Duration::ZERO {
            endpoint = endpoint.connect_timeout(self.grpc_connect_timeout);
        }

        if self.grpc_keepalive_time != Duration::ZERO {
            endpoint = endpoint
                .http2_keep_alive_interval(self.grpc_keepalive_time)
                .keep_alive_timeout(self.grpc_keepalive_timeout);
        }

        Ok(endpoint)
    }
}

fn wrap_with_domain(target: &str, domain: &str) -> Result<String> {
    if domain.is_empty() {
        return Ok(target.to_owned());
    }
    let mut parts = target.split(':');
    let Some(host) = parts.next() else {
        return Err(crate::Error::StringError(format!(
            "target {target} is not valid"
        )));
    };
    let Some(port) = parts.next() else {
        return Err(crate::Error::StringError(format!(
            "target {target} is not valid"
        )));
    };
    if parts.next().is_some() {
        return Err(crate::Error::StringError(format!(
            "target {target} is not valid"
        )));
    }
    Ok(format!("{host}.{domain}:{port}"))
}

impl SecurityManager {
    fn dial_addr(&self, addr: &str) -> Result<String> {
        let addr = SCHEME_REG.replace(addr, "");
        match self.grpc_custom_dns_domain.as_deref() {
            Some(domain) if !domain.is_empty() => wrap_with_domain(addr.as_ref(), domain),
            _ => Ok(addr.to_string()),
        }
    }
}

#[derive(Clone)]
struct CustomDnsResolver {
    resolver: TokioAsyncResolver,
}

impl CustomDnsResolver {
    fn new(dns_server: SocketAddr) -> Self {
        let ips = [dns_server.ip()];
        let name_servers = NameServerConfigGroup::from_ips_clear(
            &ips,
            dns_server.port(),
            true, /* trust_nx */
        );
        let config = ResolverConfig::from_parts(None, vec![], name_servers);
        let mut opts = ResolverOpts::default();
        // client-go uses a 10s dial timeout in util.GetCustomDNSDialer.
        opts.timeout = Duration::from_millis(10_000);
        let resolver = TokioAsyncResolver::tokio(config, opts);

        CustomDnsResolver { resolver }
    }
}

impl Service<Name> for CustomDnsResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let resolver = self.resolver.clone();
        Box::pin(async move {
            let addrs = resolver
                .lookup_ip(name.as_str())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let addrs: Vec<SocketAddr> = addrs.iter().map(|ip| SocketAddr::new(ip, 0)).collect();
            Ok(addrs.into_iter())
        })
    }
}

fn new_custom_dns_connector(dns_server: SocketAddr) -> HttpConnector<CustomDnsResolver> {
    let mut http = HttpConnector::new_with_resolver(CustomDnsResolver::new(dns_server));
    http.enforce_http(false);
    http.set_nodelay(true);
    http.set_keepalive(Some(Duration::from_secs(10)));
    http
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    use std::convert::Infallible;
    use std::sync::Arc;

    use hyper::service::service_fn;
    use hyper::Body;
    use hyper::Request;
    use hyper::Response;
    use tokio::net::TcpListener;
    use tokio::net::UdpSocket;
    use tokio::sync::Mutex;

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
        assert_eq!(mgr.grpc_connect_timeout(), Duration::from_secs(5));
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

    #[test]
    fn test_security_manager_with_grpc_connect_timeout_overrides() {
        let mgr = SecurityManager::default().with_grpc_connect_timeout(Duration::from_secs(1));
        assert_eq!(mgr.grpc_connect_timeout(), Duration::from_secs(1));
    }

    #[test]
    fn test_wrap_with_domain() {
        assert_eq!(
            wrap_with_domain("pd0.pd:2379", "cluster.local").unwrap(),
            "pd0.pd.cluster.local:2379"
        );
        assert!(wrap_with_domain("pd0.pd", "cluster.local").is_err());
        assert!(wrap_with_domain("pd0.pd:2379:extra", "cluster.local").is_err());
    }

    #[tokio::test]
    async fn test_custom_dns_connect_wraps_domain_and_uses_dns_server() {
        // Start a minimal DNS server (UDP) that maps any A query to 127.0.0.1 and records the
        // queried name.
        let dns_socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let dns_addr = dns_socket.local_addr().unwrap();
        let seen_query_name: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let seen_query_name_task = Arc::clone(&seen_query_name);

        let dns_task = tokio::spawn(async move {
            let mut buf = [0u8; 1500];
            for _ in 0..8 {
                let (n, peer) = dns_socket.recv_from(&mut buf).await.unwrap();
                let packet = &buf[..n];
                if packet.len() < 12 {
                    continue;
                }

                let (qname, q_end) = parse_qname(packet, 12).unwrap();
                {
                    let mut guard = seen_query_name_task.lock().await;
                    if guard.is_none() {
                        *guard = Some(qname.clone());
                    }
                }

                if packet.len() < q_end + 4 {
                    continue;
                }
                let qtype = u16::from_be_bytes([packet[q_end], packet[q_end + 1]]);
                let question = &packet[12..q_end + 4];

                let resp = build_dns_response(packet[0..2].try_into().unwrap(), question, qtype);
                let _ = dns_socket.send_to(&resp, peer).await.unwrap();
            }
        });

        // Start a minimal HTTP/2 server which allows tonic's channel handshake to succeed.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let svc = service_fn(|_req: Request<Body>| async move {
                Ok::<_, Infallible>(Response::new(Body::empty()))
            });
            let _ = hyper::server::conn::Http::new()
                .http2_only(true)
                .serve_connection(io, svc)
                .await;
        });

        // Dial a fake hostname and verify that DNS queries see the wrapped domain.
        let mgr = SecurityManager::default()
            .with_grpc_custom_dns(Some(dns_addr), Some("cluster.local".to_owned()));
        let _channel = mgr
            .connect(&format!("pd0.pd:{}", server_addr.port()), |ch| ch)
            .await
            .unwrap();

        // Wait for DNS query to be recorded.
        let query_name = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(name) = seen_query_name.lock().await.clone() {
                    break name;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(query_name, "pd0.pd.cluster.local");

        dns_task.abort();
        server_task.abort();
    }

    fn parse_qname(packet: &[u8], mut i: usize) -> Option<(String, usize)> {
        let mut labels = Vec::new();
        loop {
            let len = *packet.get(i)? as usize;
            i += 1;
            if len == 0 {
                break;
            }
            let label = packet.get(i..i + len)?;
            labels.push(std::str::from_utf8(label).ok()?.to_owned());
            i += len;
        }
        Some((labels.join("."), i))
    }

    fn build_dns_response(id: [u8; 2], question: &[u8], qtype: u16) -> Vec<u8> {
        let answer = qtype == 1; // A
        let ancount: u16 = if answer { 1 } else { 0 };

        let mut resp = Vec::with_capacity(12 + question.len() + if answer { 16 } else { 0 });
        resp.extend_from_slice(&id);
        resp.extend_from_slice(&0x8180u16.to_be_bytes()); // standard response, no error
        resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
        resp.extend_from_slice(&ancount.to_be_bytes()); // ANCOUNT
        resp.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
        resp.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
        resp.extend_from_slice(question);

        if answer {
            // NAME: pointer to the question name at offset 12 (0xC00C)
            resp.extend_from_slice(&[0xC0, 0x0C]);
            resp.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
            resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
            resp.extend_from_slice(&1u32.to_be_bytes()); // TTL
            resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH
            resp.extend_from_slice(&[127, 0, 0, 1]); // RDATA
        }

        resp
    }
}
