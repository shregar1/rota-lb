//! TLS/mTLS support for backends.
//!
//! This module provides TLS integration using `rustls`. It includes:
//! - `TlsConfig` for configuring TLS settings
//! - `TlsBackend` wrapper that adds TLS to any backend

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, IpAddr, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsConnector;

use crate::traits::backend::{Backend, Connection};
use crate::constants::DEFAULT_ALPN_PROTOCOLS;
use crate::error::Error;

/// TLS configuration for a backend.
#[derive(Debug)]
pub struct TlsConfig {
    /// Server name for SNI and certificate verification.
    pub server_name: String,
    /// Custom root certificates (CA bundle). If None, uses system trust store.
    pub root_certs: Option<Vec<CertificateDer<'static>>>,
    /// Client certificate and private key for mTLS.
    pub client_cert: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    /// Whether to verify the server certificate hostname.
    pub verify_hostname: bool,
    /// Connection timeout.
    pub connect_timeout: Option<std::time::Duration>,
    /// ALPN protocols to negotiate.
    pub alpn_protocols: Vec<Vec<u8>>,
}

impl TlsConfig {
    /// Create a new TLS config with just a server name.
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            root_certs: None,
            client_cert: None,
            verify_hostname: true,
            connect_timeout: None,
            alpn_protocols: DEFAULT_ALPN_PROTOCOLS.iter().map(|p| p.to_vec()).collect(),
        }
    }

    /// Add custom root certificates (CA bundle).
    #[must_use]
    pub fn with_root_certs(mut self, certs: Vec<CertificateDer<'static>>) -> Self {
        self.root_certs = Some(certs);
        self
    }

    /// Add client certificate and private key for mTLS.
    #[must_use]
    pub fn with_client_cert(
        mut self,
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        self.client_cert = Some((certs, key));
        self
    }

    /// Disable hostname verification (for testing only).
    #[must_use]
    pub const fn danger_bypass_hostname_check_only(mut self) -> Self {
        self.verify_hostname = false;
        self
    }

    /// Set connection timeout.
    #[must_use]
    pub const fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set ALPN protocols.
    #[must_use]
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Build the rustls `ClientConfig`, consuming the client cert.
    ///
    /// SNI (Server Name Indication) is always enabled (rustls 0.23 default).
    /// If `verify_hostname` is false, certificate hostname verification is
    /// skipped with a custom `ServerCertVerifier`, but SNI keeps working so
    /// the server knows which certificate to present.
    pub fn build_client_config(self) -> Result<Arc<ClientConfig>, Error> {
        let mut root_store = RootCertStore::empty();

        if let Some(ref certs) = self.root_certs {
            for cert in certs {
                root_store
                    .add(cert.clone())
                    .map_err(|e| Error::backend(format!("failed to add root cert: {e}")))?;
            }
        } else {
            let certs = rustls_native_certs::load_native_certs()
                .map_err(|e| Error::backend(format!("failed to load native certs: {e}")))?;
            for cert in certs {
                root_store
                    .add(cert)
                    .map_err(|e| Error::backend(format!("failed to add root cert: {e}")))?;
            }
        }

        let verifier: Arc<dyn ServerCertVerifier> = if self.verify_hostname {
            WebPkiServerVerifier::builder(Arc::new(root_store.clone()))
                .build()
                .map_err(|e| Error::backend(format!("verifier builder: {e}")))?
        } else {
            let inner = WebPkiServerVerifier::builder(Arc::new(root_store.clone()))
                .build()
                .map_err(|e| Error::backend(format!("verifier builder: {e}")))?;
            Arc::new(NoHostnameVerifier(inner))
        };

        let builder = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier);

        let mut config = if let Some((certs, key)) = self.client_cert {
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::backend(format!("client cert: {e}")))?
        } else {
            builder.with_no_client_auth()
        };

        config.alpn_protocols = self.alpn_protocols;
        Ok(Arc::new(config))
    }
}

/// Wraps a verifier but skips the hostname check by substituting a fixed IP.
///
/// Certificate chain validation is still performed — only the hostname
/// match is bypassed. This is the correct behaviour for `danger_bypass_hostname_check_only`.
#[derive(Debug)]
struct NoHostnameVerifier(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let any_name = ServerName::IpAddress(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED.into()));
        self.0
            .verify_server_cert(end_entity, intermediates, &any_name, ocsp, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

pub use crate::enums::tls::TlsError;

/// A backend that wraps another backend with TLS.
pub struct TlsBackend {
    inner: Box<dyn Backend>,
    server_name: String,
    tls_connector: TlsConnector,
}

impl std::fmt::Debug for TlsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsBackend")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl TlsBackend {
    /// Wrap a backend with TLS.
    pub fn new(inner: Box<dyn Backend>, tls_config: TlsConfig) -> Result<Self, Error> {
        let server_name = tls_config.server_name.clone();
        let client_config = tls_config.build_client_config()?;
        let connector = TlsConnector::from(client_config);
        Ok(Self {
            inner,
            server_name,
            tls_connector: connector,
        })
    }

    async fn connect_tls(
        connector: TlsConnector,
        server_name: String,
        conn: Connection,
    ) -> Result<Connection, Error> {
        let dns_name = rustls::pki_types::DnsName::try_from(server_name.as_str())
            .map_err(|e| Error::backend(format!("invalid server name: {e}")))?
            .to_owned();
        let server_name = ServerName::DnsName(dns_name);

        let tls_stream = connector
            .connect(server_name, conn)
            .await
            .map_err(|e| Error::backend(format!("TLS handshake failed: {e}")))?;

        Ok(Box::pin(TlsConnection { stream: tls_stream }))
    }
}

#[async_trait]
impl Backend for TlsBackend {
    async fn dial(&self, addr: &str) -> Result<Connection, Error> {
        let conn = self.inner.dial(addr).await?;
        Self::connect_tls(self.tls_connector.clone(), self.server_name.clone(), conn).await
    }

    async fn shutdown(&mut self) {
        self.inner.shutdown().await;
    }
}

/// A TLS-wrapped connection.
pub struct TlsConnection {
    stream: tokio_rustls::client::TlsStream<Connection>,
}

impl std::fmt::Debug for TlsConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConnection").finish_non_exhaustive()
    }
}

impl AsyncRead for TlsConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
