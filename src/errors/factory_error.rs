use std::fmt;

#[derive(Debug)]
pub struct FactoryError(pub String);

impl fmt::Display for FactoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "backend factory failed: {}", self.0)
    }
}

impl std::error::Error for FactoryError {}
