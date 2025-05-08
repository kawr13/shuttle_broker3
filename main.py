import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from core.config import settings # Уже импортировано
from core.logging_config import setup_logging, logger # Уже импортировано
from api.endpoints import router as api_router, router  # Уже импортировано
from services.shuttle_comms import start_shuttle_listener_server # Уже импортировано
from core.redis_client import init_redis_pool, close_redis_pool # Уже импортировано
from crud.shuttle_crud import init_shuttle_states_redis # Уже импортировано
from services.command_processor import command_processor_worker # Уже импортировано

setup_logging()  # Вызываем настройку логирования


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск шлюза WMS-Шаттл (Версия 2.0)...")
    await init_redis_pool()
    await init_shuttle_states_redis()

    asyncio.create_task(start_shuttle_listener_server())
    logger.info("TCP сервер для шаттлов запущен.")

    for i in range(settings.COMMAND_PROCESSOR_WORKERS):
        task = asyncio.create_task(command_processor_worker(worker_id=i + 1))
        command_processor_tasks_main.append(task)
    logger.info(f"{settings.COMMAND_PROCESSOR_WORKERS} воркеров обработки команд запущены.")
    yield
    logger.info("Остановка шлюза WMS-Шаттл (Версия 2.0)...")
    for task in command_processor_tasks_main:
        task.cancel()
    try:
        await asyncio.gather(*command_processor_tasks_main, return_exceptions=True)
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

# Добавляем инструментатор Prometheus
# Инициализируем его перед добавлением эндпоинтов, чтобы он мог их инструментировать
# prometheus_instrumentator = Instrumentator()
# prometheus_instrumentator.instrument(app).expose(app, include_in_schema=False, should_gzip=True)
# Лучше так, чтобы он не экспонировал метрики на всех роутерах по умолчанию
Instrumentator(
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/metrics"]  # Не инструментируем сам эндпоинт метрик
).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False, should_gzip=True)

command_processor_tasks_main = []  # Переименовал, чтобы не конфликтовать

app.include_router(router, prefix=settings.API_V1_STR)  # Используем router из endpoints


@app.get("/", summary="Health Check", include_in_schema=False)  # Скрываем из OpenAPI
async def root_health_check():  # Переименовал
    return {"message": f"{settings.PROJECT_NAME} активен"}

# Для запуска из командной строки, если этот файл запускается напрямую:
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.GATEWAY_HOST, port=settings.GATEWAY_PORT, log_level=settings.LOG_LEVEL.lower())