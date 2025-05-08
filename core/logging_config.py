import logging
import sys
from core.config import settings # Уже импортировано выше, если это один файл

def setup_logging():
    logging.basicConfig(
        level=settings.LOG_LEVEL.upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Уменьшаем "болтливость" некоторых библиотек
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.INFO)


logger = logging.getLogger(settings.PROJECT_NAME)
