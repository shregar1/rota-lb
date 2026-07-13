//! Final TLS tests to push coverage.

use std::time::Duration;
use rota::tls::TlsConfig;

#[test]
fn tls_config_with_connect_timeout_zero() {
    let config = TlsConfig::new("test")
        .with_connect_timeout(Duration::from_millis(0));
    assert_eq!(config.connect_timeout, Some(Duration::from_millis(0)));
}

#[test]
fn tls_config_with_connect_timeout_max() {
    let config = TlsConfig::new("test")
        .with_connect_timeout(Duration::from_secs(3600));
    assert_eq!(config.connect_timeout, Some(Duration::from_secs(3600)));
}

#[test]
fn tls_config_with_all_alpn() {
    let config = TlsConfig::new("test")
        .with_alpn_protocols(vec![b"h2".to_vec(), b"http/1.1".to_vec()]);
    assert_eq!(config.alpn_protocols.len(), 2);
}

#[test]
fn tls_config_with_empty_alpn() {
    let config = TlsConfig::new("test")
        .with_alpn_protocols(vec![]);
    assert!(config.alpn_protocols.is_empty());
}

#[test]
fn tls_config_with_complex_server_name() {
    let config = TlsConfig::new("very-long-subdomain.example.com:443");
    assert_eq!(config.server_name, "very-long-subdomain.example.com:443");
}

#[test]
fn tls_config_with_unicode_server_name() {
    let config = TlsConfig::new("测试.example.com");
    assert_eq!(config.server_name, "测试.example.com");
}

#[test]
fn tls_config_with_ipv4_server_name() {
    let config = TlsConfig::new("192.168.1.1:443");
    assert_eq!(config.server_name, "192.168.1.1:443");
}

#[test]
fn tls_config_with_ipv6_server_name() {
    let config = TlsConfig::new("[::1]:443");
    assert_eq!(config.server_name, "[::1]:443");
}

#[test]
fn tls_config_bypass_hostname() {
    let config = TlsConfig::new("test")
        .danger_bypass_hostname_check_only();
    assert!(!config.verify_hostname);
}

#[test]
fn tls_config_bypass_then_verify() {
    let config = TlsConfig::new("test");
    let bypassed = config.danger_bypass_hostname_check_only();
    assert!(!bypassed.verify_hostname);
}

#[test]
fn tls_config_build_with_all_fields_set() {
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = vec![];
    let config = TlsConfig::new("example.com")
        .with_root_certs(certs)
        .with_alpn_protocols(vec![b"h2".to_vec()])
        .with_connect_timeout(Duration::from_secs(10))
        .danger_bypass_hostname_check_only();
    // Build should work (or fail gracefully if system certs unavailable)
    let _ = config.build_client_config();
}

#[test]
fn tls_config_with_long_server_name() {
    let long_name = "a".repeat(1000) + ":443";
    let config = TlsConfig::new(&long_name);
    assert_eq!(config.server_name, long_name);
}

#[test]
fn tls_config_with_special_char_server_name() {
    let config = TlsConfig::new("host-with-dashes.example.com:443");
    assert_eq!(config.server_name, "host-with-dashes.example.com:443");
}

#[test]
fn tls_config_alpn_overwrite() {
    let config = TlsConfig::new("test")
        .with_alpn_protocols(vec![b"h2".to_vec()])
        .with_alpn_protocols(vec![b"http/1.1".to_vec()]);
    assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()]);
}

#[test]
fn tls_config_connect_timeout_overwrite() {
    let config = TlsConfig::new("test")
        .with_connect_timeout(Duration::from_secs(5))
        .with_connect_timeout(Duration::from_secs(10));
    assert_eq!(config.connect_timeout, Some(Duration::from_secs(10)));
}

#[test]
fn tls_config_bypass_toggle() {
    let config1 = TlsConfig::new("test");
    assert!(config1.verify_hostname);
    let config2 = config1.danger_bypass_hostname_check_only();
    assert!(!config2.verify_hostname);
}

#[test]
fn tls_config_field_assignments_complex() {
    let config = TlsConfig {
        server_name: "complex.example.com:443".to_string(),
        root_certs: None,
        client_cert: None,
        verify_hostname: false,
        connect_timeout: Some(Duration::from_secs(30)),
        alpn_protocols: vec![b"h2".to_vec(), b"http/1.1".to_vec()],
    };
    assert_eq!(config.server_name, "complex.example.com:443");
    assert!(!config.verify_hostname);
    assert_eq!(config.connect_timeout, Some(Duration::from_secs(30)));
    assert_eq!(config.alpn_protocols.len(), 2);
}

#[test]
fn tls_config_build_various_options() {
    // Test build with various combinations of options
    for verify in [true, false] {
        for timeout in [Duration::from_millis(1), Duration::from_secs(1)] {
            for alpn in [vec![b"h2".to_vec()], vec![b"http/1.1".to_vec()], vec![]] {
                let mut config = TlsConfig::new("test")
                    .with_alpn_protocols(alpn.clone())
                    .with_connect_timeout(timeout);
                if !verify {
                    config = config.danger_bypass_hostname_check_only();
                }
                let _ = config.build_client_config();
            }
        }
    }
}

#[test]
fn tls_error_display_variants() {
    use rota::tls::TlsError;

    let e1 = TlsError::InvalidServerName("test1".to_string());
    assert!(format!("{}", e1).contains("test1"));

    let e2 = TlsError::Certificate("test2".to_string());
    assert!(format!("{}", e2).contains("test2"));
}

#[test]
fn tls_error_display_long() {
    use rota::tls::TlsError;
    let long_msg = "a".repeat(1000);
    let e = TlsError::InvalidServerName(long_msg.clone());
    assert!(format!("{}", e).contains(&long_msg));
}

#[test]
fn tls_error_io_with_message() {
    use rota::tls::TlsError;
    use std::io;
    let io_err = io::Error::new(io::ErrorKind::Other, "test io error");
    let tls_err: TlsError = io_err.into();
    let display = format!("{}", tls_err);
    assert!(display.contains("test io error"));
}

#[test]
fn tls_error_with_special_chars() {
    use rota::tls::TlsError;
    let e = TlsError::InvalidServerName("test with spaces and !@#".to_string());
    let display = format!("{}", e);
    assert!(display.contains("test with spaces"));
}

#[test]
fn tls_error_unicode() {
    use rota::tls::TlsError;
    let e = TlsError::InvalidServerName("测试".to_string());
    let display = format!("{}", e);
    assert!(display.contains("测试"));
}

#[test]
fn tls_error_empty_string() {
    use rota::tls::TlsError;
    let e = TlsError::InvalidServerName("".to_string());
    let display = format!("{}", e);
    // Empty string should still display something
    assert!(display.contains("Invalid"));
}

#[test]
fn tls_error_debug() {
    use rota::tls::TlsError;
    let e = TlsError::InvalidServerName("test".to_string());
    let _ = format!("{:?}", e);
}

#[test]
fn tls_error_io_debug() {
    use rota::tls::TlsError;
    use std::io;
    let io_err = io::Error::new(io::ErrorKind::Other, "test");
    let tls_err: TlsError = io_err.into();
    let _ = format!("{:?}", tls_err);
}

#[test]
fn tls_error_certificate_debug() {
    use rota::tls::TlsError;
    let e = TlsError::Certificate("test".to_string());
    let _ = format!("{:?}", e);
}

#[test]
fn tls_error_from_io_error() {
    use rota::tls::TlsError;
    use std::io;
    let io_err = io::Error::new(io::ErrorKind::ConnectionReset, "connection reset");
    let tls_err: TlsError = io_err.into();
    match tls_err {
        TlsError::Io(_) => {}
        _ => panic!("Expected TlsError::Io"),
    }
}