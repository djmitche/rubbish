use failure::Fail;
use std::io;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "IO Error: {}", _0)]
    IOError(#[cause] io::Error),
}
