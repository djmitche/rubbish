use std;
use mio_more;

error_chain! {
    errors {
    }

    foreign_links {
        Io(::std::io::Error);
        AddrParseError(std::net::AddrParseError);
        TryRecvError(std::sync::mpsc::TryRecvError);
        TimerError(mio_more::timer::TimerError);
    }
}
