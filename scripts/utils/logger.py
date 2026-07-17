import os
import logging
from logging.handlers import RotatingFileHandler
from utils.config import LOGS_DIR

def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured standard Logger that logs both to console (stdout)
    and a rotating log file under the logs/ directory.
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if logger is retrieved multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s')
        
        # 1. Console Handler (for Docker and Airflow capturing)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 2. Rotating File Handler (for log persistence)
        log_file_path = os.path.join(LOGS_DIR, "pipeline.log")
        file_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    return logger
