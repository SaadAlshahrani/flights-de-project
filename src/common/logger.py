import logging
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

def setup_logger(name: str, logfile: str):
    logger = logging.getLogger(name)
    logger.setLevel(level=logging.INFO)

    if logger.handlers:
        return logger
    
    handler = logging.FileHandler(LOG_DIR / logfile, encoding="utf-8")
    formatter = logging.Formatter(
        style="{",
        fmt="{asctime} - {levelname} - {name} - {message}",
        datefmt="%Y-%m-%d %H:%M",
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger