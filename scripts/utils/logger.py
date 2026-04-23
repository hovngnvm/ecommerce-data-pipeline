import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from scripts.config.settings import settings

def get_logger(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a standard Logger configured to write to both stdout
    and a rotating log file in the logs directory.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S%z'
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        log_dir = Path(settings.logs_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / "pipeline.log"
        file_handler = RotatingFileHandler(str(log_file_path), maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger
