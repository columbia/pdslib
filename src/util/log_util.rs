use log4rs;

pub fn init() {
    log4rs::init_file("logging_config.yaml", Default::default()).unwrap();
}