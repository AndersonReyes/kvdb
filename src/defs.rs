use failure::Fail;
use std::io;

/// Different types of error that can happen in the db
#[derive(Fail, Debug)]
pub enum KvdbError {
    /// Generic db error
    #[fail(display = "There was an error in the database: {}", _0)]
    KvdbError(String),
    /// DB IO error
    #[fail(display = "IO Error: {}", _0)]
    IO(#[cause] io::Error),
    /// DB IO error
    #[fail(display = "IO Error: {}", _0)]
    SerializationError(#[cause] serde_json::Error),
}

impl From<io::Error> for KvdbError {
    fn from(e: io::Error) -> Self {
        Self::IO(e)
    }
}

impl From<serde_json::Error> for KvdbError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerializationError(e)
    }
}

/// Database error alias
pub type Result<T> = ::std::result::Result<T, KvdbError>;
