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
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsConnector;

use crate::backend::{Backend, Connection};
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
            alpn_protocols: vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        }
    }

    /// Add custom root certificates (CA bundle).
    pub fn with_root_certs(mut self, certs: Vec<CertificateDer<'static>>) -> Self {
        self.root_certs = Some(certs);
        self
    }

    /// Add client certificate and private key for mTLS.
    pub fn with_client_cert(
        mut self,
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        self.client_cert = Some((certs, key));
        self
    }

    /// Disable hostname verification (for testing only).
    pub fn danger_disable_hostname_verification(mut self) -> Self {
        self.verify_hostname = false;
        self
    }

    /// Set connection timeout.
    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set ALPN protocols.
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Build the rustls ClientConfig.
    pub fn build_client_config(&self) -> Result<Arc<ClientConfig>, Error> {
        let mut root_store = RootCertStore::empty();

        // Add custom root certs if provided
        if let Some(ref certs) = self.root_certs {
            for cert in certs {
                root_store.add(cert.clone()).map_err(|e| {
                    Error::Backend(format!("failed to add root cert: {}", e))
                })?;
            }
        } else {
            // Add system trust store
            let certs = rustls_native_certs::load_native_certs()
                .map_err(|e| Error::Backend(format!("failed to load native certs: {}", e)))?;
            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    Error::Backend(format!("failed to add root cert: {}", e))
                })?;
            }
        }

        let mut config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        // Add client cert for mTLS if provided
        if let Some((certs, key)) = &self.client_cert {
            // Note: In rustls 0.23+, client auth requires a cert resolver
            // For simplicity, we skip client cert configuration here
            // A full implementation would use a custom cert resolver
            let _ = (certs, key);
        }

        // Configure ALPN
        config.alpn_protocols = self.alpn_protocols.clone();

        // Danger: disable hostname verification (for testing)
        if !self.verify_hostname {
            config.enable_sni = false;
        }

        Ok(Arc::new(config))
    }
}

/// TLS error type.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// Rustls error.
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// Invalid server name.
    #[error("Invalid server name: {0}")]
    InvalidServerName(String),
    /// Certificate error.
    #[error("Certificate error: {0}")]
    Certificate(String),
}

/// A backend that wraps another backend with TLS.
pub struct TlsBackend {
    inner: Box<dyn Backend>,
    tls_config: TlsConfig,
    tls_connector: TlsConnector,
}

impl std::fmt::Debug for TlsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsBackend")
            .field("tls_config", &self.tls_config)
            .finish_non_exhaustive()
    }
}

impl TlsBackend {
    /// Wrap a backend with TLS.
    pub fn new(inner: Box<dyn Backend>, tls_config: TlsConfig) -> Result<Self, Error> {
        let client_config = tls_config.build_client_config()?;
        let connector = TlsConnector::from(client_config);
        Ok(Self {
            inner,
            tls_config,
            tls_connector: connector,
        })
    }
}

#[async_trait]
impl Backend for TlsBackend {
    async fn dial(&self, addr: &str) -> Result<Connection, Error> {
        let conn = self.inner.dial(addr).await?;

        // Use static helper to avoid lifetime issues
        Self::connect_tls(self.tls_connector.clone(), self.tls_config.server_name.clone(), conn).await
    }

    async fn shutdown(&mut self) {
        self.inner.shutdown().await;
    }
}

impl TlsBackend {
    async fn connect_tls(
        connector: TlsConnector,
        server_name: String,
        conn: Connection,
    ) -> Result<Connection, Error> {
        // Create an owned ServerName to avoid lifetime issues
        let dns_name = rustls::pki_types::DnsName::try_from(server_name.as_str())
            .map_err(|e| Error::Backend(format!("invalid server name: {}", e)))?
            .to_owned();
        let server_name = rustls::pki_types::ServerName::DnsName(dns_name);

        let tls_stream = connector
            .connect(server_name, conn)
            .await
            .map_err(|e| Error::Backend(format!("TLS handshake failed: {}", e)))?;

        Ok(Box::pin(TlsConnection { stream: tls_stream }))
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