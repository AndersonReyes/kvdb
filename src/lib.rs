/// Library of key value database
#[deny(missing_docs)]
pub mod defs;
pub mod store;

mod log;

pub use self::defs::{KvdbError, Result};
pub use self::store::KvStore;
