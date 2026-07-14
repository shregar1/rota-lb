//! Tests for the TLS module.

#![cfg(feature = "tls")]

use rota_lb::tls::TlsConfig;
use std::time::Duration;

#[test]
fn tls_config_new() {
    let config = TlsConfig::new("api.example.com");
    assert_eq!(config.server_name, "api.example.com");
    assert!(config.verify_hostname);
    assert!(config.root_certs.is_none());
    assert!(config.client_cert.is_none());
    assert!(config.connect_timeout.is_none());
}

#[test]
fn tls_config_with_root_certs() {
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = vec![];
    let config = TlsConfig::new("example.com").with_root_certs(certs);
    assert!(config.root_certs.is_some());
}

#[test]
fn tls_config_with_alpn_protocols() {
    let protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let config = TlsConfig::new("example.com").with_alpn_protocols(protocols.clone());
    assert_eq!(config.alpn_protocols, protocols);
}

#[test]
fn tls_config_with_danger_bypass_hostname_check() {
    let config = TlsConfig::new("example.com").danger_bypass_hostname_check_only();
    assert!(!config.verify_hostname);
}

#[test]
fn tls_config_with_connect_timeout() {
    let config = TlsConfig::new("example.com").with_connect_timeout(Duration::from_secs(5));
    assert_eq!(config.connect_timeout, Some(Duration::from_secs(5)));
}

#[test]
fn tls_config_debug() {
    let config = TlsConfig::new("example.com");
    let _ = format!("{:?}", config);
}
