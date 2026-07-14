//! Tests for the error module.

use rota_lb::error::Error;
use std::io;

#[test]
fn error_display() {
    let e1 = Error::NoBackends;
    assert!(format!("{}", e1).contains("no backends"));

    let e2 = Error::Factory("test factory error".into());
    assert!(format!("{}", e2).contains("test factory error"));

    let e3 = Error::Backend("test backend error".into());
    assert!(format!("{}", e3).contains("test backend error"));

    let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
    let e4 = Error::from(io_err);
    assert!(format!("{}", e4).contains("refused"));

    let e5 = Error::InvalidAddress {
        addr: "bad".to_string(),
        reason: "no port",
    };
    let s = format!("{}", e5);
    assert!(s.contains("bad"));
    assert!(s.contains("no port"));
}

#[test]
fn error_debug() {
    let e = Error::NoBackends;
    let _ = format!("{:?}", e);
}

#[test]
fn error_from_io() {
    let io_err = io::Error::other("test");
    let e: Error = io_err.into();
    match e {
        Error::Io(_) => {}
        _ => panic!("Expected Error::Io"),
    }
}

#[test]
fn error_from_io_chain() {
    let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "denied");
    let e: Error = io_err.into();
    let display = format!("{}", e);
    assert!(display.contains("denied"));
}

#[test]
fn error_clone_via_display() {
    let e = Error::Factory("cloneable".into());
    let s = format!("{}", e);
    assert!(s.contains("cloneable"));
}

#[test]
fn error_backend_helper() {
    let e = Error::backend("test");
    match e {
        Error::Backend(s) => assert_eq!(s, "test"),
        _ => panic!("Expected Error::Backend"),
    }
}

#[test]
fn error_factory_helper() {
    let e = Error::factory("test");
    match e {
        Error::Factory(s) => assert_eq!(s, "test"),
        _ => panic!("Expected Error::Factory"),
    }
}

#[test]
fn error_invalid_address_struct() {
    let e = Error::InvalidAddress {
        addr: "x".to_string(),
        reason: "y",
    };
    match e {
        Error::InvalidAddress { addr, reason } => {
            assert_eq!(addr, "x");
            assert_eq!(reason, "y");
        }
        _ => panic!("Expected Error::InvalidAddress"),
    }
}
