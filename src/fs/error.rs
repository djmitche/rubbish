use cas;

error_chain! {
    errors {
    }   

    foreign_links {
        Cas(cas::Error);
    }   
}
