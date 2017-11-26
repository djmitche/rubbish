//! The `rubbish::net::transport` module implemnts a Tokio transport for communication between
//! Rubbish nodes.

use std::io;
use std::str;
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};

pub(super) struct Codec;

impl Decoder for Codec {
    type Item = String;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<String>> {
        if let Some(i) = buf.iter().position(|&b| b == b'\n') {
            // remove the serialized frame from the buffer.
            let line = buf.split_to(i);

            // Also remove the '\n'
            buf.split_to(1);

            // Turn this data into a UTF string and return it in a Frame.
            match str::from_utf8(&line) {
                Ok(s) => Ok(Some(s.to_string())),
                Err(_) => Err(io::Error::new(io::ErrorKind::Other, "invalid UTF-8")),
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder for Codec {
    type Item = String;
    type Error = io::Error;

    fn encode(&mut self, msg: String, buf: &mut BytesMut) -> io::Result<()> {
        buf.extend(msg.as_bytes());
        buf.extend(b"\n");
        Ok(())
    }
}

pub(super) fn new() -> Codec {
    Codec
}

#[cfg(test)]
mod test {
    use super::Codec;
    use tokio_io::codec::Decoder;
    use util::test::init_env_logger;
    use bytes::BytesMut;

    #[test]
    fn test() {
        init_env_logger();

        let mut codec = Codec;
        let mut bytes: BytesMut = "abc\ndef\ng".into();
        assert_eq!(codec.decode(&mut bytes).unwrap(), Some("abc".into()));
        assert_eq!(codec.decode(&mut bytes).unwrap(), Some("def".into()));
        assert_eq!(codec.decode(&mut bytes).unwrap(), None);
    }
}
