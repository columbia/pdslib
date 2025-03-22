use log4rs;

pub fn init_default_logging() {
    log4rs::init_file("logging_config.yaml", Default::default()).unwrap();
}
