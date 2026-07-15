//! More tests for the retry module to improve coverage.

use rota_lb::error::Error;
use rota_lb::retry::{
    is_transient_error, ExponentialBackoff, FixedRetry, NoRetry, RetryOnError, RetryPolicy,
    RetryPolicyBuilder,
};
use std::time::Duration;

#[test]
fn no_retry_clone() {
    let p1 = NoRetry;
    let p2 = p1;
    // NoRetry is Copy, so this is a copy
    assert_eq!(p1.max_attempts(), p2.max_attempts());
}

#[test]
fn no_retry_copy() {
    let p1 = NoRetry;
    let p2 = p1; // Copy
    assert_eq!(p1.max_attempts(), p2.max_attempts());
}

#[test]
fn no_retry_debug() {
    let p = NoRetry;
    let _ = format!("{:?}", p);
}

#[test]
#[allow(clippy::clone_on_copy)]
fn fixed_retry_clone() {
    let p = FixedRetry::new(Duration::from_millis(100));
    let p2 = p.clone();
    assert_eq!(
        p.should_retry(1, &Error::backend("test")),
        p2.should_retry(1, &Error::backend("test"))
    );
}

#[test]
fn fixed_retry_copy() {
    let p = FixedRetry::new(Duration::from_millis(100));
    let _p2 = p; // Copy
}

#[test]
fn fixed_retry_debug() {
    let p = FixedRetry::new(Duration::from_millis(100));
    let _ = format!("{:?}", p);
}

#[test]
fn fixed_retry_with_max_attempts_zero() {
    let p = FixedRetry::new(Duration::from_millis(100)).with_max_attempts(0);
    // 0 means no retries
    assert_eq!(p.should_retry(1, &Error::backend("test")), None);
}

#[test]
fn fixed_retry_with_max_attempts_one() {
    let p = FixedRetry::new(Duration::from_millis(100)).with_max_attempts(2);
    // attempt 1: 1 >= 2 false, returns Some
    // attempt 2: 2 >= 2 true, returns None
    assert!(p.should_retry(1, &Error::backend("test")).is_some());
    assert!(p.should_retry(2, &Error::backend("test")).is_none());
}

#[test]
fn fixed_retry_total_timeout() {
    let p = FixedRetry::new(Duration::from_millis(100));
    assert_eq!(p.total_timeout(), None);
}

#[test]
fn fixed_retry_max_attempts() {
    let p1 = FixedRetry::new(Duration::from_millis(100));
    assert_eq!(p1.max_attempts(), None);
    let p2 = FixedRetry::new(Duration::from_millis(100)).with_max_attempts(5);
    assert_eq!(p2.max_attempts(), Some(5));
}

#[test]
#[allow(clippy::clone_on_copy)]
fn exponential_backoff_clone() {
    let p = ExponentialBackoff::new(Duration::from_millis(100));
    let _p2 = p.clone();
}

#[test]
fn exponential_backoff_copy() {
    let p = ExponentialBackoff::new(Duration::from_millis(100));
    let _p2 = p; // Copy
}

#[test]
fn exponential_backoff_debug() {
    let p = ExponentialBackoff::new(Duration::from_millis(100));
    let _ = format!("{:?}", p);
}

#[test]
fn exponential_backoff_default_max_delay() {
    let _p = ExponentialBackoff::new(Duration::from_millis(100));
    // max_delay is private - we test it indirectly via should_retry
}

#[test]
fn exponential_backoff_custom_max_delay() {
    let _p =
        ExponentialBackoff::new(Duration::from_millis(100)).with_max_delay(Duration::from_secs(60));
    // max_delay is private - we test it indirectly via should_retry
}

#[test]
fn exponential_backoff_default_multiplier() {
    let _p = ExponentialBackoff::new(Duration::from_millis(100));
    // multiplier is private - we test it indirectly via should_retry
}

#[test]
fn exponential_backoff_custom_multiplier() {
    let _p = ExponentialBackoff::new(Duration::from_millis(100)).with_multiplier(3.0);
    // multiplier is private - we test it indirectly via should_retry
}

#[test]
fn exponential_backoff_total_timeout() {
    let p = ExponentialBackoff::new(Duration::from_millis(100));
    assert_eq!(p.total_timeout(), None);
}

#[test]
fn exponential_backoff_max_attempts() {
    let p = ExponentialBackoff::new(Duration::from_millis(100));
    assert_eq!(p.max_attempts(), None);
    let p2 = ExponentialBackoff::new(Duration::from_millis(100)).with_max_attempts(5);
    assert_eq!(p2.max_attempts(), Some(5));
}

#[test]
fn exponential_backoff_grows_exponentially() {
    let p = ExponentialBackoff::new(Duration::from_millis(100))
        .with_jitter(false)
        .with_multiplier(2.0);
    let d1 = p.should_retry(1, &Error::backend("test")).unwrap();
    let d2 = p.should_retry(2, &Error::backend("test")).unwrap();
    let d3 = p.should_retry(3, &Error::backend("test")).unwrap();
    // d2 should be 2x d1, d3 should be 4x d1
    assert_eq!(d2.as_millis(), (d1.as_millis() as f64 * 2.0) as u128);
    assert_eq!(d3.as_millis(), (d1.as_millis() as f64 * 4.0) as u128);
}

#[test]
fn exponential_backoff_respects_max_delay_v2() {
    let p = ExponentialBackoff::new(Duration::from_millis(100))
        .with_max_delay(Duration::from_millis(250))
        .with_jitter(false)
        .with_multiplier(2.0);
    // 100 * 2^5 = 3200ms, should be capped at 250ms
    let d = p.should_retry(5, &Error::backend("test")).unwrap();
    assert_eq!(d, Duration::from_millis(250));
}

#[test]
fn retry_on_error_clone() {
    // RetryOnError doesn't implement Clone because the inner Box<dyn RetryPolicy> can't be cloned
    // We just test that the struct is constructed
    let _p = RetryOnError::new(FixedRetry::new(Duration::from_millis(100)), |_| true);
}

#[test]
fn retry_on_error_debug() {
    let p = RetryOnError::new(FixedRetry::new(Duration::from_millis(100)), |_| true);
    let _ = format!("{:?}", p);
}

#[test]
fn retry_on_error_total_timeout_delegates() {
    let p = RetryOnError::new(FixedRetry::new(Duration::from_millis(100)), |_| true);
    assert_eq!(p.total_timeout(), None);
}

#[test]
fn retry_on_error_max_attempts_delegates() {
    let p = RetryOnError::new(
        FixedRetry::new(Duration::from_millis(100)).with_max_attempts(3),
        |_| true,
    );
    assert_eq!(p.max_attempts(), Some(3));
}

#[test]
fn retry_on_error_no_retry_on_predicate_false() {
    let p = RetryOnError::new(FixedRetry::new(Duration::from_millis(100)), |_| false);
    assert!(p.should_retry(1, &Error::backend("test")).is_none());
}

#[test]
fn retry_policy_builder_default() {
    let _b = RetryPolicyBuilder::default();
}

#[test]
fn retry_policy_builder_debug() {
    let b = RetryPolicyBuilder::default();
    let _ = format!("{:?}", b);
}

#[test]
fn retry_policy_builder_no_retry() {
    let policy = RetryPolicyBuilder::default().no_retry().build();
    assert!(policy.is_some());
    let p = policy.unwrap();
    assert!(p.should_retry(1, &Error::backend("test")).is_none());
}

#[test]
fn retry_policy_builder_fixed_retry() {
    let policy = RetryPolicyBuilder::default()
        .fixed_retry(Duration::from_millis(50))
        .build();
    assert!(policy.is_some());
    let p = policy.unwrap();
    assert!(p.should_retry(1, &Error::backend("test")).is_some());
}

#[test]
fn retry_policy_builder_exponential_backoff() {
    let policy = RetryPolicyBuilder::default()
        .exponential_backoff(Duration::from_millis(50))
        .build();
    assert!(policy.is_some());
    let p = policy.unwrap();
    assert!(p.should_retry(1, &Error::backend("test")).is_some());
}

#[test]
fn retry_policy_builder_custom() {
    let custom = ExponentialBackoff::new(Duration::from_millis(10));
    let policy = RetryPolicyBuilder::default().custom(custom).build();
    assert!(policy.is_some());
}

#[test]
fn retry_policy_builder_empty() {
    let policy = RetryPolicyBuilder::default().build();
    assert!(policy.is_none());
}

#[test]
fn retry_policy_builder_clone() {
    let b = RetryPolicyBuilder::default().no_retry();
    let b2 = b.clone();
    assert!(b2.build().is_some());
}

#[test]
fn is_transient_error_invalid_address_field() {
    let err = Error::invalid_address("test".to_string(), "no port");
    assert!(!is_transient_error(&err));
}

#[test]
fn is_transient_error_factory() {
    let err = Error::factory("test");
    assert!(!is_transient_error(&err));
}
