suppressWarnings(suppressPackageStartupMessages({
  library(logger)
  library(dotenv)
}))

load_dot_env("global-disclosure-data.env")
log_file <- Sys.getenv("LOG_FILE")

if (!is.na(log_file) & log_file != "" & log_file != "stdout") {
  log_appender(appender_file(Sys.getenv("LOG_FILE")))
}
