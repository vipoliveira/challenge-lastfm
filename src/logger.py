import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Creates and configures a logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a handler to write to stdout
    handler = logging.StreamHandler(sys.stdout)
    
    # Create a formatter and set it for the handler
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    # Add the handler to the logger
    if not logger.handlers:
        logger.addHandler(handler)
        
    return logger
