use cas;
use std;

error_chain! {
    errors {
    }   

    foreign_links {
        Cas(cas::Error);
    }   
}

impl std::convert::From<Error> for std::fmt::Error {
    fn from(_: Error) -> Self {
        std::fmt::Error
    }
}
