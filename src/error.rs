use std::io;

pub(crate) type Result<T> = std::result::Result<T, RaftError>;

#[derive(Debug)]
pub(crate) struct RaftError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl RaftError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            code: "error",
            message: message.into(),
        }
    }

    pub(crate) fn coded(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Process exit code for this error. The fine-grained category lives in
    /// `code` (surfaced in --json mode); the exit code stays coarse so shell
    /// callers can branch on the common cases.
    pub(crate) fn exit_code(&self) -> i32 {
        match self.code {
            "timeout" => 2,
            _ => 1,
        }
    }
}

impl std::fmt::Display for RaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RaftError {}

impl From<io::Error> for RaftError {
    fn from(value: io::Error) -> Self {
        Self::coded("io", value.to_string())
    }
}

impl From<serde_json::Error> for RaftError {
    fn from(value: serde_json::Error) -> Self {
        Self::coded("parse", value.to_string())
    }
}

#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::error::RaftError::new(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! bail_code {
    ($code:literal, $($arg:tt)*) => {
        return Err($crate::error::RaftError::coded($code, format!($($arg)*)))
    };
}
