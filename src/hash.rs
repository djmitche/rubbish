use crypto::digest::Digest;
use crypto::sha2::Sha256;

/// Hash the given content, returning the string form
pub fn sha256(content: &str) -> String {
    let mut sha = Sha256::new();
    sha.input_str(content);
    return sha.result_str();
}

#[cfg(test)]
mod tests {
    #[test]
    fn sha256_empty_value() {
        assert_eq!(super::sha256(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    #[test]
    fn sha256_simple_value() {
        assert_eq!(super::sha256("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }
}
