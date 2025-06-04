from typing import Optional
import redis.asyncio as redis
from core.config import settings
from core.logging_config import logger

redis_client_instance: Optional[redis.Redis] = None


async def init_redis_pool():

    global redis_client_instance
    try:
        redis_client_instance = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True
        )
        await redis_client_instance.ping()
        logger.info("Успешное подключение к Redis.")
    except Exception as e:
        logger.error(f"Не удалось подключиться к Redis: {e}")
        redis_client_instance = None

async def close_redis_pool():
    from core.logging_config import logger
    if redis_client_instance:
        await redis_client_instance.close()
        logger.info("Соединение с Redis закрыто.")

def get_redis_client() -> redis.Redis:
    if not redis_client_instance:
        raise ConnectionError("Клиент Redis не инициализирован или соединение потеряно.")
    return redis_client_instance
