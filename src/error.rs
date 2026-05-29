use std::io;

pub(crate) type Result<T> = std::result::Result<T, RaftError>;

#[derive(Debug)]
pub(crate) struct RaftError(pub(crate) String);

impl std::fmt::Display for RaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RaftError {}

impl From<io::Error> for RaftError {
    fn from(value: io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<serde_json::Error> for RaftError {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}

#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::error::RaftError(format!($($arg)*)))
    };
}
