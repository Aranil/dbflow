import logging
import os
import configparser

# Load config.ini
config = configparser.ConfigParser()
config.read("config.ini")

# module name
MODULE_NAME = "dbflow"

# Get log file path dynamically
log_file = config.get("logging", "log_file", fallback=f"../../_logs/{MODULE_NAME}.log")
log_level = config.get("logging", "level", fallback="WARNING").upper()

os.makedirs(os.path.dirname(log_file), exist_ok=True)


# Configure logging
logging.basicConfig(
    level=getattr(logging, log_level),
    format=config.get("logging", "format", fallback="%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.FileHandler(log_file),  # Log to file
        logging.StreamHandler()  # Also log to console
    ]
)

# Create a logger for the module
logger = logging.getLogger(MODULE_NAME)
