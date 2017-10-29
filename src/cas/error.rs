
error_chain! {
    errors {
        LockError(msg: String) {
            description("lock error")
            display("lock error: '{}'", msg)
        }
    }   

    foreign_links {
    }   
}
