import asyncio
import random
from typing import Callable, Any, TypeVar, Optional
from functools import wraps

from core.logging_config import logger
from prometheus_client import Counter, Histogram

T = TypeVar('T')

# Метрики для мониторинга повторных попыток
WMS_API_RETRIES = Counter(
    'wms_api_retries_total', 
    'Total number of WMS API request retries',
    ['endpoint', 'status']
)

WMS_API_REQUEST_DURATION = Histogram(
    'wms_api_request_duration_seconds',
    'Duration of WMS API requests',
    ['endpoint', 'status']
)

async def retry_async(
    func: Callable[..., Any],
    *args,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    jitter: float = 0.1,
    endpoint: str = None,
    retry_on: tuple = (Exception,),
    **kwargs
) -> Any:
    """
    Выполняет асинхронную функцию с повторными попытками при ошибке.
    
    Args:
        func: Асинхронная функция для выполнения
        *args: Аргументы функции
        max_retries: Максимальное количество повторных попыток
        base_delay: Начальная задержка между попытками в секундах
        max_delay: Максимальная задержка между попытками в секундах
        jitter: Коэффициент случайности для предотвращения одновременных запросов
        endpoint: Название эндпоинта для метрик
        retry_on: Типы исключений, при которых нужно повторять попытки
        **kwargs: Именованные аргументы функции
    
    Returns:
        Результат выполнения функции
    
    Raises:
        Exception: Последнее исключение, если все попытки не удались
    """
    endpoint_name = endpoint or func.__name__
    
    retries = 0
    last_exception = None
    
    while retries <= max_retries:
        try:
            start_time = asyncio.get_event_loop().time()
            result = await func(*args, **kwargs)
            duration = asyncio.get_event_loop().time() - start_time
            WMS_API_REQUEST_DURATION.labels(endpoint=endpoint_name, status="success").observe(duration)
            return result
        except retry_on as e:
            last_exception = e
            retries += 1
            
            if retries > max_retries:
                WMS_API_REQUEST_DURATION.labels(endpoint=endpoint_name, status="failure").observe(
                    asyncio.get_event_loop().time() - start_time
                )
                logger.error(f"Все попытки выполнения {func.__name__} не удались после {max_retries} попыток")
                raise last_exception
            
            # Экспоненциальная задержка с добавлением случайности
            delay = min(base_delay * (2 ** (retries - 1)), max_delay)
            jitter_value = random.uniform(-jitter * delay, jitter * delay)
            delay = delay + jitter_value
            
            WMS_API_RETRIES.labels(endpoint=endpoint_name, status="retry").inc()
            logger.warning(f"Попытка {retries}/{max_retries} для {func.__name__} не удалась: {e}. "
                          f"Повторная попытка через {delay:.2f} секунд")
            await asyncio.sleep(delay)