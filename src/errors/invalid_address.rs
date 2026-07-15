use std::fmt;

#[derive(Debug)]
pub struct InvalidAddress {
    pub addr: String,
    pub reason: &'static str,
}

impl fmt::Display for InvalidAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid dial address {:?}: {}", self.addr, self.reason)
    }
}

impl std::error::Error for InvalidAddress {}
