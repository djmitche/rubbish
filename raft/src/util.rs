use std::io::{Read, Write};

pub(crate) fn writeall<W: Write>(stream: &mut W, buf: &[u8]) -> std::io::Result<()> {
    let mut pos = 0;
    while pos < buf.len() {
        pos += stream.write(&buf[pos..])?;
    }
    Ok(())
}

pub(crate) fn readall<R: Read>(stream: &mut R, buf: &mut [u8]) -> std::io::Result<()> {
    let mut pos = 0;
    while pos < buf.len() {
        pos += stream.read(&mut buf[pos..])?;
    }
    Ok(())
}
