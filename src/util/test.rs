use std::sync::{Once, ONCE_INIT};
use env_logger;

static LOGGER_INITIALIZED: Once = ONCE_INIT;

/// Initialize `env_logger` on the first call, and do nothing thereafter.  This is useful in tests
/// where the order of the tests is not defined, but each requires `env_logger`.
pub fn init_env_logger() {
    LOGGER_INITIALIZED.call_once(|| env_logger::init().unwrap());
}
