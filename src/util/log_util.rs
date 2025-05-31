use std::sync::Once;
use log4rs;

static LOGGER_INIT: Once = Once::new();

/// Initializes the `log4rs` logger using `logging_config.yaml`.
pub fn init() {
    LOGGER_INIT.call_once(|| {
        // Initialize from YAML config file
        match log4rs::init_file("logging_config.yaml", Default::default()) {
            Ok(_) => {
                // Standard `log` after successful init
                log::info!(
                    "pdslib logging initialized successfully from logging_config.yaml."
                );
            }
            Err(e) => {
                // Logging not set up, stderr
                eprintln!(
                    "ERROR: Failed to initialize logger from logging_config.yaml: {}", e
                );
                eprintln!("Falling back to basic stdout logging (Trace level).");

                // Fallback - set up a basic console logger
                let stdout_appender = log4rs::append::console::ConsoleAppender::builder()
                    .encoder(Box::new(
                        log4rs::encode::pattern::PatternEncoder::new(
                            "{h({d(%Y-%m-%d %H:%M:%S)(utc)} - {l}: {m}{n})}",
                        ),
                    ))
                    .build();

                // Build the fallback config
                match log4rs::config::Config::builder()
                    .appender(
                        log4rs::config::Appender::builder()
                            .build("stdout", Box::new(stdout_appender)),
                    )
                    .build(
                        log4rs::config::Root::builder()
                            .appender("stdout")
                            .build(log::LevelFilter::Trace),
                    ) {
                    Ok(config) => {
                        // Attempt to initialize with the fallback configuration
                        if let Err(init_err) = log4rs::init_config(config) {
                            eprintln!("ERROR: Failed to initialize fallback logger: {}. No logging will be available.", init_err);
                        } else {
                            log::warn!(
                                "pdslib logging initialized using basic fallback (stdout, Trace level)."
                            );
                        }
                    }
                    Err(build_err) => {
                        eprintln!("ERROR: Failed to build fallback logging configuration: {}. No logging will be available.", build_err);
                    }
                }
            }
        }
    });
}
