use failure::Fail;
use std::io;
use std::str;
use std::string::FromUtf8Error;

/// Different types of error that can happen in the db
#[derive(Fail, Debug)]
pub enum KvdbError {
    /// Generic db error
    #[fail(display = "There was an error in the database: {}", _0)]
    KvdbError(String),
    /// DB IO error
    #[fail(display = "IO Error: {}", _0)]
    IO(#[cause] io::Error),
    /// Serde error
    #[fail(display = "IO Error: {}", _0)]
    SerializationError(#[cause] serde_json::Error),
    /// Parsing raw data before sending to Serde error
    #[fail(display = "Parsing Error: {}", _0)]
    ParsingError(#[cause] str::Utf8Error),
}

impl From<io::Error> for KvdbError {
    fn from(e: io::Error) -> Self {
        Self::IO(e)
    }
}

impl From<FromUtf8Error> for KvdbError {
    fn from(e: FromUtf8Error) -> Self {
        Self::ParsingError(e.utf8_error())
    }
}

impl From<serde_json::Error> for KvdbError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerializationError(e)
    }
}

/// Database error alias
pub type Result<T> = ::std::result::Result<T, KvdbError>;
/// byte offset in the log
pub type LogOffset = u64;
