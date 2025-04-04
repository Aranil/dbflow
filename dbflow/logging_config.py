import logging
import os
import configparser



CONFIG_LOCATIONS = [
    "./config.ini",
    "../config.ini",
    ".../config.ini"
]


# Find the first existing config.ini
config_file = next((f for f in CONFIG_LOCATIONS if os.path.exists(f)), None)


# Initialize config parser
config = configparser.ConfigParser(interpolation=None)

# Read the config file if found
if config_file:
    config.read(config_file)


# module name
MODULE_NAME = "dbflow"


# Looks for level under [logging] in config.ini. If it exists, it grabs the value from config.ini, else sets default defined here
log_file = config.get("logging", "log_file", fallback=f"../../_logs/{MODULE_NAME}.log")
log_level_str = config.get("logging", "level", fallback="WARNING").upper()


# Validate log level
try:
    log_level = getattr(logging, log_level_str)
except AttributeError:
    raise ValueError(f"Invalid log level: {log_level_str}")


# Ensure the log directory exists and is writable
log_dir = os.path.dirname(log_file)
os.makedirs(log_dir, exist_ok=True)
if not os.access(log_dir, os.W_OK):
    raise PermissionError(f"Cannot write to log directory: {log_dir}")


# Use the format value from [logging] in config.ini, or fallback to this hardcoded format if it's missing.
logging.basicConfig(
    level=log_level,
    format=config.get("logging", "format", fallback="%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.FileHandler(log_file),  # Log to file
        logging.StreamHandler()        # Also log to console
    ]
)

# Create a logger for the module
logger = logging.getLogger(MODULE_NAME)

# Warn if config.ini wasn't found
if not config_file:
    logger.warning("config.ini not found! Using default values.")

# confirm logger started
logger.info(f"Logger initialized with level '{log_level_str}' and log file at '{log_file}'")

