use crate::cas;
use failure::Fail;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Lock Error: {}", _0)]
    CasError(#[cause] cas::Error),
}
