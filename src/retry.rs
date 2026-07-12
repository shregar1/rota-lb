//! Retry policy for dial operations.

use std::time::Duration;
use crate::error::Error;

/// A policy for retrying failed dial attempts.
///
/// This trait allows customizing retry behavior for transient failures.
/// The default implementation provides exponential backoff with jitter.
pub trait RetryPolicy: Send + Sync {
    /// Determine whether to retry and the delay before the next attempt.
    ///
    /// Returns `Some(delay)` if a retry should be attempted after `delay`,
    /// or `None` if the operation should fail with the given error.
    fn should_retry(&self, attempt: u32, error: &Error) -> Option<Duration>;

    /// Maximum total time allowed for all retry attempts.
    ///
    /// If the total elapsed time exceeds this budget, no more retries are attempted.
    fn total_timeout(&self) -> Option<Duration> {
        None
    }

    /// Maximum number of retry attempts (including the initial attempt).
    ///
    /// If `None`, there's no limit on the number of attempts (bounded by `total_timeout`).
    fn max_attempts(&self) -> Option<u32> {
        None
    }
}

/// No retries - fail immediately on any error.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRetry;

impl RetryPolicy for NoRetry {
    fn should_retry(&self, _attempt: u32, _error: &Error) -> Option<Duration> {
        None
    }
}

/// Retry with fixed delay between attempts.
#[derive(Debug, Clone, Copy)]
pub struct FixedRetry {
    delay: Duration,
    max_attempts: Option<u32>,
}

impl FixedRetry {
    /// Create a new fixed retry policy.
    pub fn new(delay: Duration) -> Self {
        Self { delay, max_attempts: None }
    }

    /// Set the maximum number of attempts.
    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = Some(max);
        self
    }
}

impl RetryPolicy for FixedRetry {
    fn should_retry(&self, attempt: u32, _error: &Error) -> Option<Duration> {
        if let Some(max) = self.max_attempts {
            if attempt >= max {
                return None;
            }
        }
        Some(self.delay)
    }
}

/// Retry with exponential backoff and optional jitter.
#[derive(Debug, Clone, Copy)]
pub struct ExponentialBackoff {
    base_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
    max_attempts: Option<u32>,
    jitter: bool,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff retry policy.
    pub fn new(base_delay: Duration) -> Self {
        Self {
            base_delay,
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: None,
            jitter: true,
        }
    }

    /// Set the maximum delay between retries.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Set the multiplier for exponential growth.
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Set the maximum number of attempts.
    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = Some(max);
        self
    }

    /// Enable or disable jitter (default: true).
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn should_retry(&self, attempt: u32, _error: &Error) -> Option<Duration> {
        if let Some(max) = self.max_attempts {
            if attempt >= max {
                return None;
            }
        }

        let delay = self.base_delay.as_secs_f64() * self.multiplier.powi(attempt as i32);
        let delay = Duration::from_secs_f64(delay.min(self.max_delay.as_secs_f64()));

        if self.jitter {
            // Add jitter: random value between 0 and delay
            let jitter = rand::random::<f64>();
            Some(Duration::from_secs_f64(delay.as_secs_f64() * (0.5 + jitter * 0.5)))
        } else {
            Some(delay)
        }
    }
}

/// Retry only on specific error types (e.g., transient I/O errors).
pub struct RetryOnError {
    inner: Box<dyn RetryPolicy>,
    predicate: Box<dyn Fn(&Error) -> bool + Send + Sync>,
}

impl std::fmt::Debug for RetryOnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryOnError").finish_non_exhaustive()
    }
}

impl RetryOnError {
    /// Create a new conditional retry policy.
    pub fn new(inner: impl RetryPolicy + 'static, predicate: impl Fn(&Error) -> bool + Send + Sync + 'static) -> Self {
        Self {
            inner: Box::new(inner),
            predicate: Box::new(predicate),
        }
    }
}

impl RetryPolicy for RetryOnError {
    fn should_retry(&self, attempt: u32, error: &Error) -> Option<Duration> {
        if (self.predicate)(error) {
            self.inner.should_retry(attempt, error)
        } else {
            None
        }
    }

    fn total_timeout(&self) -> Option<Duration> {
        self.inner.total_timeout()
    }

    fn max_attempts(&self) -> Option<u32> {
        self.inner.max_attempts()
    }
}

/// Default retry predicate: retry on I/O errors and timeout errors.
pub fn is_transient_error(error: &Error) -> bool {
    match error {
        Error::Io(_) => true,
        Error::Backend(msg) => msg.contains("timeout"),
        _ => false,
    }
}

/// A builder for configuring retry policies on the LoadBalancer.
pub struct RetryPolicyBuilder {
    policy: Option<Box<dyn RetryPolicy>>,
}

impl std::fmt::Debug for RetryPolicyBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryPolicyBuilder").finish_non_exhaustive()
    }
}

impl Default for RetryPolicyBuilder {
    fn default() -> Self {
        Self { policy: None }
    }
}

impl RetryPolicyBuilder {
    /// Disable retries (fail fast).
    pub fn no_retry(mut self) -> Self {
        self.policy = Some(Box::new(NoRetry));
        self
    }

    /// Use fixed delay between retries.
    pub fn fixed_retry(mut self, delay: Duration) -> Self {
        self.policy = Some(Box::new(FixedRetry::new(delay)));
        self
    }

    /// Use exponential backoff with jitter.
    pub fn exponential_backoff(mut self, base_delay: Duration) -> Self {
        self.policy = Some(Box::new(ExponentialBackoff::new(base_delay)));
        self
    }

    /// Set a custom retry policy.
    pub fn custom(mut self, policy: impl RetryPolicy + 'static) -> Self {
        self.policy = Some(Box::new(policy));
        self
    }

    /// Build the retry policy.
    pub fn build(self) -> Option<Box<dyn RetryPolicy>> {
        self.policy
    }
}