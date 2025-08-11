import os
import logging
from logging.config import dictConfig
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Settings:
    def __init__(self):
        # Base directory (current file's parent)
        self.BASE_DIR = Path(__file__).resolve(strict=True).parent
        self.INPUT_DIR = Path(os.getenv("INPUT_DIR"))
        self.OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR"))

        self.INPUT_CLAIMS_CSV = str(self.INPUT_DIR / os.getenv("INPUT_CLAIMS_CSV"))
        self.INPUT_POLICYHOLDER_CSV = str(self.INPUT_DIR / os.getenv("INPUT_POLICYHOLDER_CSV"))
        self.OUTPUT_PROCESSED_CLAIMS_CSV = str(self.OUTPUT_DIR / os.getenv("OUTPUT_PROCESSED_CLAIMS_CSV"))

        # Logging configuration dict
        self.LOGS = {
            'version': 1,
            'disable_existing_loggers': True,
            'formatters': {
                'default': {
                    '()': 'logging.Formatter',
                    'fmt': '%(levelname)s %(asctime)s %(name)s: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S',
                },
            },
            'handlers': {
                'console': {
                    'level': 'DEBUG',
                    'formatter': 'default',
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stderr',
                },
                'logfile': {
                    'level': 'INFO',
                    'formatter': 'default',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': str(self.BASE_DIR / "logs/pipeline.log"),
                    'maxBytes': 10 * 1024 * 1024,  # 10MB
                    'backupCount': 3,
                    'mode': 'a',
                },
            },
            'root': {
                'level': 'INFO',
                'handlers': ['console', 'logfile'],
            },
            'loggers': {
                'pipeline': {
                    'level': 'INFO',
                    'handlers': ['console', 'logfile'],
                    'propagate': False,
                },
            },
        }

    class Config:
        env_file = '.env'


# Create logs directory if it doesn't exist
settings = Settings()
logs_dir = settings.BASE_DIR / "logs"
logs_dir.mkdir(exist_ok=True)

# Apply logging config
dictConfig(settings.LOGS)

# Create logger instance
logger = logging.getLogger('pipeline')
