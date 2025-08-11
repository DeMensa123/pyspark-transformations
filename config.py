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
        self.INPUT_DIR = os.getenv("INPUT_DIR", str(self.BASE_DIR / "data"))
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", str(self.BASE_DIR / "output"))
        self.INPUT_CLAIMS_CSV = os.getenv(str(Path(self.INPUT_DIR) / "INPUT_CLAIMS_CSV"), str(Path(self.INPUT_DIR) / "claims_data.csv"))
        self.INPUT_POLICYHOLDER_CSV = os.getenv(str(Path(self.INPUT_DIR) / "INPUT_POLICYHOLDER_CSV"), str(Path(self.INPUT_DIR) / "policyholder_data.csv"))
        self.OUTPUT_PROCESSED_CLAIMS_CSV = os.getenv(str(Path(self.OUTPUT_DIR) / "OUTPUT_PROCESSED_CLAIMS_CSV"), str(Path(self.OUTPUT_DIR) / "processed_claims.csv"))

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
