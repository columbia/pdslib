use log4rs;

pub fn init_default_logging() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
}
