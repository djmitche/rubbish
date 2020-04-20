use failure::Fail;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Lock Error: {}", _0)]
    LockError(String),
}
