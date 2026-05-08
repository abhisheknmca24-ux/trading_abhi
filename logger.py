import logging
import os

# Create a custom logger
logger = logging.getLogger("trading_bot")

# Check environment
is_production = bool(os.getenv("RAILWAY_ENVIRONMENT"))

# Set log level based on environment
if is_production:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.DEBUG)

# Create handlers
c_handler = logging.StreamHandler()

# Create formatters and add it to handlers
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
c_handler.setFormatter(log_format)

# Add handlers to the logger
if not logger.handlers:
    logger.addHandler(c_handler)

# Disable duplicate logs
logger.propagate = False
