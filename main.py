import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from core.config import settings
from core.logging_config import setup_logging, logger
from api.endpoints import router as api_router
from services.shuttle_comms import start_shuttle_listener_server
from core.redis_client import init_redis_pool, close_redis_pool
from crud.shuttle_crud import init_shuttle_states_redis
from services.command_processor import command_processor_worker, initialize_shuttle_locks, initialize_shuttle_queues
from services.wms_mock import mock_router


setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск шлюза WMS-Шаттл (Версия 2.0)...")
    await settings.load_config()
    # Инициализация Redis
    await init_redis_pool()
    await initialize_shuttle_locks()
    await initialize_shuttle_queues()
     # Загружаем конфигурацию из Redis
    await init_shuttle_states_redis()
    logger.info("Redis инициализирован, состояния шаттлов и конфигурация загружены.")

    # Запуск TCP сервера для шаттлов
    asyncio.create_task(start_shuttle_listener_server())
    logger.info("TCP сервер для шаттлов запущен на порту 8181.")

    # Запуск воркеров для обработки команд
    command_processor_tasks = []
    for i in range(settings.COMMAND_PROCESSOR_WORKERS):
        task = asyncio.create_task(command_processor_worker(worker_id=i + 1))
        command_processor_tasks.append(task)
    logger.info(f"{settings.COMMAND_PROCESSOR_WORKERS} воркеров обработки команд запущены.")
    
    # Запуск мониторинга состояния шаттлов (heartbeat)
    from services.heartbeat_monitor import heartbeat_monitor
    await heartbeat_monitor.start()
    logger.info("Мониторинг состояния шаттлов (heartbeat) запущен.")
    
    # Запуск интеграции с WMS API, если она включена
    if settings.WMS_INTEGRATION_ENABLED:
        from services.wms_integration import wms_integration
        await wms_integration.start()
        logger.info(f"Интеграция с WMS API запущена (интервал опроса: {settings.WMS_POLL_INTERVAL} сек)")

    yield

    # Остановка шлюза
    logger.info("Остановка шлюза WMS-Шаттл (Версия 2.0)...")
    
    # Остановка интеграции с WMS API
    if settings.WMS_INTEGRATION_ENABLED:
        from services.wms_integration import wms_integration
        await wms_integration.stop()
        logger.info("Интеграция с WMS API остановлена")
    
    # Остановка мониторинга состояния шаттлов
    from services.heartbeat_monitor import heartbeat_monitor
    await heartbeat_monitor.stop()
    logger.info("Мониторинг состояния шаттлов остановлен.")
    
    for task in command_processor_tasks:
        task.cancel()
    try:
        await asyncio.gather(*command_processor_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.info("Задачи воркеров были отменены.")
    logger.info("Воркеры обработки команд остановлены.")
    await close_redis_pool()
    logger.info("Соединение с Redis закрыто.")


app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

# Настройка Prometheus для мониторинга
Instrumentator(
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/metrics"]
).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False, should_gzip=True)

# Подключение маршрутов API
app.include_router(api_router, prefix=settings.API_V1_STR)
app.include_router(mock_router)


@app.get("/", summary="Health Check", include_in_schema=False)
async def root():
    return {"message": f"{settings.PROJECT_NAME} активен"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.GATEWAY_HOST, port=settings.GATEWAY_PORT, log_level=settings.LOG_LEVEL.lower())
