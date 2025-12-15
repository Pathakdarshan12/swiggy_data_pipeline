import logging
from pathlib import Path
from datetime import datetime

def setup_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """Set up logger with file and console handlers"""

    # Create logs directory
    Path(log_dir).mkdir(exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Remove existing handlers
    logger.handlers = []

    # File handler
    log_file = f"{log_dir}/{name}_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger