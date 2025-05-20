import asyncio
import time
from typing import Dict, Optional

from icecream import ic

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import get_shuttle_state_crud, update_shuttle_state_crud
from models.shuttle import ShuttleCommand, ShuttleOperationalStatus
from services.services import COMMANDS_SENT_TOTAL, COMMAND_QUEUE_SIZE_METRIC
from services.shuttle_comms import send_command_to_shuttle

shuttle_locks: Dict[str, asyncio.Lock] = {}
shuttle_queues: Dict[str, asyncio.PriorityQueue] = {}

# Команды, которые обрабатываются всегда, даже если шаттл занят
ALLOWED_WHEN_BUSY_COMMANDS = {
    ShuttleCommand.HOME,
    ShuttleCommand.STATUS,
    ShuttleCommand.MRCD,
    ShuttleCommand.BATTERY,
    ShuttleCommand.WDH,
    ShuttleCommand.WLH
}

async def initialize_shuttle_queues():
    global shuttle_queues
    shuttle_queues = {
        shuttle_id: asyncio.PriorityQueue(maxsize=settings.COMMAND_QUEUE_MAX_SIZE)
        for shuttle_id in settings.SHUTTLES_CONFIG.keys()
    }
    logger.info(f"Shuttle queues initialized: {list(shuttle_queues.keys())}")

async def initialize_shuttle_locks():
    global shuttle_locks
    shuttle_locks = {
        shuttle_id: asyncio.Lock() for shuttle_id in settings.SHUTTLES_CONFIG.keys()
    }
    logger.info(f"Shuttle locks initialized: {list(shuttle_locks.keys())}")

async def process_wms_command_internal(shuttle_id: str, command: ShuttleCommand, params: Optional[str] = None,
                                       externaIID: Optional[str] = None) -> bool:
    shuttle_state = await get_shuttle_state_crud(shuttle_id)
    if not shuttle_state:
        logger.error(f"Шаттл {shuttle_id} не найден для команды {command.value}.")
        COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status="failure_not_found").inc()
        return False

    if command not in ALLOWED_WHEN_BUSY_COMMANDS:
        if shuttle_state.status not in [ShuttleOperationalStatus.FREE, ShuttleOperationalStatus.UNKNOWN]:
            logger.warning(f"Шаттл {shuttle_id} в состоянии {shuttle_state.status}, команда {command.value} отклонена")
            COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status="failure_busy").inc()
            return False

    if command == ShuttleCommand.HOME:
        logger.info(f"Команда HOME для {shuttle_id}. Прерываем текущую операцию, если есть.")
        if shuttle_state.current_command:
            logger.debug(f"Очистка текущей команды {shuttle_state.current_command} перед HOME")
            await update_shuttle_state_crud(shuttle_id, {"current_command": None})

    command_str_to_send = command.value
    param_for_shuttle = None
    if command in [ShuttleCommand.FIFO_NNN, ShuttleCommand.FILO_NNN]:
        if not params or not params.isdigit():
            logger.error(f"Некорректный параметр для {command.value}: {params}")
            COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value,
                                       status="failure_bad_params").inc()
            return False
        param_for_shuttle = params

    success = await send_command_to_shuttle(shuttle_id, command_str_to_send, param_for_shuttle)
    metric_status = "success" if success else "failure_send_error"
    COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status=metric_status).inc()

    if success:
        updates = {
            "last_message_received_from_wms": command.value + (f"-{params}" if params else ""),
            "last_seen": time.time(),
            "externaIID": externaIID  # Сохраняем externaIID для последующего поиска
        }
        if command == ShuttleCommand.HOME:
            updates["status"] = ShuttleOperationalStatus.FREE
            updates["current_command"] = None
        elif command != ShuttleCommand.MRCD:
            updates["current_command"] = command.value + (f"-{params}" if params else "")
        await update_shuttle_state_crud(shuttle_id, updates)
        logger.debug(f"Состояние шаттла {shuttle_id} обновлено после команды {command.value}")
    return success

async def add_command_to_queue(shuttle_id: str, command: ShuttleCommand,
                              params: Optional[str] = None, externaIID: Optional[str] = None, priority: int = 10):
    if command in ALLOWED_WHEN_BUSY_COMMANDS:
        # Немедленная обработка для команд, разрешённых при BUSY
        async with shuttle_locks.get(shuttle_id, asyncio.Lock()):
            success = await process_wms_command_internal(
                shuttle_id=shuttle_id,
                command=command,
                params=params,
                externaIID=externaIID
            )
        if success:
            logger.info(f"Команда {command.value} для {shuttle_id} обработана немедленно.")
        else:
            logger.error(f"Не удалось обработать команду {command.value} для {shuttle_id}.")
        return success

    # Обычные команды добавляются в приоритетную очередь
    data = (
        priority,  # Приоритет (меньше = выше)
        {
            "shuttle_id": shuttle_id,
            "command": command,
            "params": params,
            "externaIID": externaIID
        }
    )
    try:
        await shuttle_queues[shuttle_id].put(data)
        COMMAND_QUEUE_SIZE_METRIC.set(sum(q.qsize() for q in shuttle_queues.values()))
        logger.info(f"Команда {command.value} для {shuttle_id} добавлена в очередь с приоритетом {priority}.")
        return True
    except asyncio.QueueFull:
        logger.error(f"Очередь команд для {shuttle_id} заполнена. Команда {command.value} не добавлена.")
        return False

async def command_processor_worker(worker_id: int):
    logger.info(f"Воркер обработки команд {worker_id} запущен.")
    while True:
        try:
            for shuttle_id in settings.SHUTTLES_CONFIG.keys():
                shuttle_lock = shuttle_locks.get(shuttle_id)
                if not shuttle_lock or shuttle_lock.locked():
                    continue

                state = await get_shuttle_state_crud(shuttle_id)
                if state.status != ShuttleOperationalStatus.FREE:
                    continue

                try:
                    priority, data = shuttle_queues[shuttle_id].get_nowait()
                    async with shuttle_lock:
                        await process_wms_command_internal(
                            shuttle_id=shuttle_id,
                            command=data["command"],
                            params=data.get("params"),
                            externaIID=data.get("externaIID")
                        )
                    shuttle_queues[shuttle_id].task_done()
                    COMMAND_QUEUE_SIZE_METRIC.set(sum(q.qsize() for q in shuttle_queues.values()))
                except asyncio.QueueEmpty:
                    pass

            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            logger.info(f"Воркер обработки команд {worker_id} останавливается.")
            break
        except Exception as e:
            logger.error(f"Ошибка в воркере {worker_id}: {e}", exc_info=True)