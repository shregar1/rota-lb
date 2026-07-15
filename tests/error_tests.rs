use rota_lb::error::Error;
use std::io;

#[test]
fn error_display() {
    let e1 = Error::no_backends();
    assert!(format!("{}", e1).contains("no backends"));

    let e2 = Error::factory("test factory error");
    assert!(format!("{}", e2).contains("test factory error"));

    let e3 = Error::backend("test backend error");
    assert!(format!("{}", e3).contains("test backend error"));

    let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
    let e4 = Error::from(io_err);
    assert!(format!("{}", e4).contains("refused"));

    let e5 = Error::invalid_address("bad".to_string(), "no port");
    let s = format!("{}", e5);
    assert!(s.contains("bad"));
    assert!(s.contains("no port"));
}

#[test]
fn error_debug() {
    let e = Error::no_backends();
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
    let e = Error::factory("cloneable");
    let s = format!("{}", e);
    assert!(s.contains("cloneable"));
}

#[test]
fn error_backend_helper() {
    let e = Error::backend("test");
    match e {
        Error::Backend(ref s) => assert_eq!(s.0, "test"),
        _ => panic!("Expected Error::Backend"),
    }
}

#[test]
fn error_factory_helper() {
    let e = Error::factory("test");
    match e {
        Error::Factory(ref s) => assert_eq!(s.0, "test"),
        _ => panic!("Expected Error::Factory"),
    }
}

#[test]
fn error_invalid_address_struct() {
    let e = Error::invalid_address("x".to_string(), "y");
    match e {
        Error::InvalidAddress(ref a) => {
            assert_eq!(a.addr, "x");
            assert_eq!(a.reason, "y");
        }
        _ => panic!("Expected Error::InvalidAddress"),
    }
}
