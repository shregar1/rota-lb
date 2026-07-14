use std::io;

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
