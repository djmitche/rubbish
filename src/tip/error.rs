use cas;
use fs;

error_chain! {
    errors {
    }   

    foreign_links {
        Cas(cas::Error);
        FS(fs::Error);
    }   
}
