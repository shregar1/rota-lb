//! Tower integration for `LoadBalancer`.
//!
//! Provides a basic `tower::Service` implementation so `LoadBalancer` can be used
//! in a tower middleware stack. The integration is gated behind the `tower` feature.
//!
//! Note: The `Service` implementation requires the `LoadBalancer` to be shared via `Arc`
//! for proper lifetime management. For full `Layer` support, wrap the `LoadBalancer`
//! in `Arc` and use `tower::util::Shared`, or create a dedicated `Layer` that builds
//! a new `LoadBalancer` per service.

#[cfg(feature = "tower")]
mod tower_impl {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use crate::error::Error;
    use crate::utils::retry::RetryPolicy;
    use tower::Service;

    /// A request for the load balancer service.
    #[derive(Clone)]
    pub struct LbRequest {
        /// The address to dial (e.g., "api.example.com:443").
        pub addr: String,
        /// Optional per-request dial timeout override.
        pub dial_timeout: Option<std::time::Duration>,
        /// Optional per-request retry policy override.
        pub retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
    }

    impl LbRequest {
        /// Create a new request with just an address.
        pub fn new(addr: impl Into<String>) -> Self {
            Self {
                addr: addr.into(),
                dial_timeout: None,
                retry_policy: None,
            }
        }

        /// Set a per-request dial timeout.
        #[must_use]
        pub const fn with_dial_timeout(mut self, timeout: std::time::Duration) -> Self {
            self.dial_timeout = Some(timeout);
            self
        }

        /// Set a per-request retry policy.
        #[must_use]
        pub fn with_retry_policy(mut self, policy: impl RetryPolicy + 'static) -> Self {
            self.retry_policy = Some(Arc::new(policy));
            self
        }
    }

    impl std::fmt::Debug for LbRequest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LbRequest")
                .field("addr", &self.addr)
                .field("dial_timeout", &self.dial_timeout)
                .finish_non_exhaustive()
        }
    }

    /// A response from the load balancer: a connection to a backend.
    pub type LbResponse = crate::services::balancer::GuardedConnection;

    // Implement Tower Service for Arc<LoadBalancer>
    // Note: This is the recommended pattern for tower services that need internal state.
    impl Service<LbRequest> for Arc<crate::services::balancer::LoadBalancer> {
        type Response = LbResponse;
        type Error = Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.backend_count() == 0 {
                Poll::Ready(Err(Error::no_backends()))
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn call(&mut self, req: LbRequest) -> Self::Future {
            let addr = req.addr;
            let dial_timeout = req.dial_timeout;
            let retry_policy = req.retry_policy;
            let lb = self.clone();
            Box::pin(async move {
                lb.dial_with_options(&addr, dial_timeout, retry_policy)
                    .await
            })
        }
    }
}

// Re-export the tower types when the feature is enabled
#[cfg(feature = "tower")]
pub use crate::services::balancer::GuardedConnection;
#[cfg(feature = "tower")]
pub use tower_impl::{LbRequest, LbResponse};
