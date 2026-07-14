//! Tests for the retry policy module.

use rota_lb::error::Error;
use rota_lb::retry::{
    is_transient_error, ExponentialBackoff, FixedRetry, NoRetry, RetryOnError, RetryPolicy,
};
use std::time::Duration;

// ============================================================================
//  NoRetry
// ============================================================================

#[test]
fn no_retry_never_retries() {
    let policy = NoRetry;
    assert_eq!(policy.should_retry(1, &Error::Backend("test".into())), None);
    assert_eq!(policy.should_retry(5, &Error::NoBackends), None);
    assert_eq!(
        policy.should_retry(100, &Error::Backend("any".into())),
        None
    );
}

#[test]
fn no_retry_default_max_attempts() {
    let policy = NoRetry;
    assert_eq!(policy.max_attempts(), None);
}

#[test]
fn no_retry_default_total_timeout() {
    let policy = NoRetry;
    assert_eq!(policy.total_timeout(), None);
}

// ============================================================================
//  FixedRetry
// ============================================================================

#[test]
fn fixed_retry_with_default_attempts() {
    let policy = FixedRetry::new(Duration::from_millis(100));
    assert_eq!(
        policy.should_retry(1, &Error::Backend("test".into())),
        Some(Duration::from_millis(100))
    );
    assert_eq!(
        policy.should_retry(2, &Error::Backend("test".into())),
        Some(Duration::from_millis(100))
    );
    assert_eq!(
        policy.should_retry(100, &Error::Backend("test".into())),
        Some(Duration::from_millis(100))
    );
}

#[tokio::test]
async fn fixed_retry_with_max_attempts() {
    let policy = FixedRetry::new(Duration::from_millis(50)).with_max_attempts(3);
    assert_eq!(
        policy.should_retry(1, &Error::Backend("test".into())),
        Some(Duration::from_millis(50))
    );
    assert_eq!(
        policy.should_retry(2, &Error::Backend("test".into())),
        Some(Duration::from_millis(50))
    );
    assert_eq!(policy.should_retry(3, &Error::Backend("test".into())), None);
    assert_eq!(policy.should_retry(4, &Error::Backend("test".into())), None);
}

#[test]
fn fixed_retry_with_zero_delay() {
    let policy = FixedRetry::new(Duration::from_millis(0));
    assert_eq!(
        policy.should_retry(1, &Error::Backend("test".into())),
        Some(Duration::from_millis(0))
    );
}

// ============================================================================
//  ExponentialBackoff
// ============================================================================

#[test]
fn exponential_backoff_grows() {
    let policy = ExponentialBackoff::new(Duration::from_millis(100));
    let d1 = policy
        .should_retry(1, &Error::Backend("test".into()))
        .unwrap();
    let d2 = policy
        .should_retry(2, &Error::Backend("test".into()))
        .unwrap();
    let d3 = policy
        .should_retry(3, &Error::Backend("test".into()))
        .unwrap();
    // Each attempt should be >= previous (with jitter, may be slightly different)
    assert!(d2 >= d1, "d2 ({:?}) should be >= d1 ({:?})", d2, d1);
    assert!(d3 >= d2, "d3 ({:?}) should be >= d2 ({:?})", d3, d2);
}

#[test]
fn exponential_backoff_respects_max_delay() {
    let policy = ExponentialBackoff::new(Duration::from_millis(100))
        .with_max_delay(Duration::from_millis(500))
        .with_jitter(false);
    let d = policy
        .should_retry(10, &Error::Backend("test".into()))
        .unwrap();
    assert!(d <= Duration::from_millis(500));
}

#[test]
fn exponential_backoff_with_multiplier() {
    let policy = ExponentialBackoff::new(Duration::from_millis(100))
        .with_multiplier(3.0)
        .with_jitter(false);
    let d1 = policy
        .should_retry(1, &Error::Backend("test".into()))
        .unwrap();
    let d2 = policy
        .should_retry(2, &Error::Backend("test".into()))
        .unwrap();
    // d2 should be approximately 3x d1
    assert!(d2 >= d1 * 2, "d2 ({:?}) should be ~3x d1 ({:?})", d2, d1);
}

#[tokio::test]
async fn exponential_backoff_with_max_attempts() {
    let policy = ExponentialBackoff::new(Duration::from_millis(10))
        .with_max_attempts(3)
        .with_jitter(false);
    // attempt 1 and 2 should retry, attempt 3 should not (3 >= max=3)
    assert!(policy
        .should_retry(1, &Error::Backend("test".into()))
        .is_some());
    assert!(policy
        .should_retry(2, &Error::Backend("test".into()))
        .is_some());
    assert!(policy
        .should_retry(3, &Error::Backend("test".into()))
        .is_none());
    assert!(policy
        .should_retry(10, &Error::Backend("test".into()))
        .is_none());
}

#[test]
fn exponential_backoff_with_jitter() {
    // With jitter, the delay can vary between 0.5x and 1.0x the computed delay
    let policy = ExponentialBackoff::new(Duration::from_millis(10)).with_jitter(true);
    for _ in 0..10 {
        let d = policy
            .should_retry(1, &Error::Backend("test".into()))
            .unwrap();
        // For attempt 1, base is 10ms * 2.0 = 20ms, with jitter 0.5x to 1.0x
        // So delay is between 10ms and 20ms
        assert!(
            d >= Duration::from_millis(5),
            "d ({:?}) should be >= 5ms",
            d
        );
        assert!(
            d <= Duration::from_millis(25),
            "d ({:?}) should be <= 25ms",
            d
        );
    }
}

#[test]
fn exponential_backoff_without_jitter_is_deterministic() {
    let policy = ExponentialBackoff::new(Duration::from_millis(100)).with_jitter(false);
    let d1 = policy
        .should_retry(1, &Error::Backend("test".into()))
        .unwrap();
    let d2 = policy
        .should_retry(1, &Error::Backend("test".into()))
        .unwrap();
    // Without jitter, same attempt gives same delay
    assert_eq!(d1, d2);
}

// ============================================================================
//  RetryOnError
// ============================================================================

#[test]
fn retry_on_error_predicate_true() {
    let policy = RetryOnError::new(FixedRetry::new(Duration::from_millis(100)), |_| true);
    assert!(policy
        .should_retry(1, &Error::Backend("test".into()))
        .is_some());
}

#[test]
fn retry_on_error_predicate_false() {
    let policy = RetryOnError::new(FixedRetry::new(Duration::from_millis(100)), |_| false);
    assert!(policy
        .should_retry(1, &Error::Backend("test".into()))
        .is_none());
}

#[test]
fn retry_on_error_predicate_specific() {
    let policy = RetryOnError::new(FixedRetry::new(Duration::from_millis(100)), |e| {
        matches!(e, Error::NoBackends)
    });
    // Should retry on NoBackends
    assert!(policy.should_retry(1, &Error::NoBackends).is_some());
    // Should not retry on other errors
    assert!(policy
        .should_retry(1, &Error::Backend("test".into()))
        .is_none());
}

#[test]
fn retry_on_error_delegates_to_inner() {
    let policy = RetryOnError::new(FixedRetry::new(Duration::from_millis(50)), |_| true);
    let d = policy.should_retry(1, &Error::NoBackends).unwrap();
    assert_eq!(d, Duration::from_millis(50));
}

// ============================================================================
//  is_transient_error
// ============================================================================

#[test]
fn is_transient_error_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
    let err = Error::from(io_err);
    assert!(is_transient_error(&err));
}

#[test]
fn is_transient_error_backend_with_timeout() {
    let err = Error::Backend("dial timeout".to_string());
    assert!(is_transient_error(&err));
}

#[test]
fn is_transient_error_backend_without_timeout() {
    let err = Error::Backend("some other error".to_string());
    assert!(!is_transient_error(&err));
}

#[test]
fn is_transient_error_no_backends() {
    let err = Error::NoBackends;
    assert!(!is_transient_error(&err));
}

#[test]
fn is_transient_error_invalid_address() {
    use rota_lb::error::Error;
    let err = Error::InvalidAddress {
        addr: "bad".to_string(),
        reason: "no port",
    };
    assert!(!is_transient_error(&err));
}
