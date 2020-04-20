use failure::Fail;
use crate::cas;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Lock Error: {}", _0)]
    CasError(#[cause] cas::Error),
}
