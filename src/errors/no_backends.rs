use std::fmt;

#[derive(Debug, Clone, Copy)]
pub struct NoBackends;

impl fmt::Display for NoBackends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no backends available — pool is empty")
    }
}

impl std::error::Error for NoBackends {}
