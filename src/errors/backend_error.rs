use std::fmt;

#[derive(Debug)]
pub struct BackendError(pub String);

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "backend operation failed: {}", self.0)
    }
}

impl std::error::Error for BackendError {}
