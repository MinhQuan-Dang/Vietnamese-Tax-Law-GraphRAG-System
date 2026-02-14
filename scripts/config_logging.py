import os
import logging
import logging.config
from lightrag.utils import logger, set_verbose_debug

def configure_logging(log_filename: str = "lightrag_tax_pipeline.log"):
    for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error", "lightrag"]:
        lg = logging.getLogger(logger_name)
        lg.handlers = []
        lg.filters = []

    log_dir = os.getenv("LOG_DIR", os.getcwd())
    os.makedirs(log_dir, exist_ok=True)

    log_file_path = os.path.abspath(os.path.join(log_dir, log_filename))
    print(f"\nLightRAG log file: {log_file_path}\n")

    log_max_bytes = int(os.getenv("LOG_MAX_BYTES", 10485760))
    log_backup_count = int(os.getenv("LOG_BACKUP_COUNT", 5))

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {"format": "%(levelname)s: %(message)s"},
                "detailed": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
            },
            "handlers": {
                "console": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                },
                "file": {
                    "formatter": "detailed",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": log_file_path,
                    "maxBytes": log_max_bytes,
                    "backupCount": log_backup_count,
                    "encoding": "utf-8",
                },
            },
            "loggers": {
                "lightrag": {
                    "handlers": ["console", "file"],
                    "level": "INFO",
                    "propagate": False,
                }
            },
        }
    )

    logger.setLevel(logging.INFO)
    set_verbose_debug(os.getenv("VERBOSE_DEBUG", "false").lower() == "true")
