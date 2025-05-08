import asyncio
import time
from typing import Dict, Optional

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import get_shuttle_state_crud, update_shuttle_state_crud
from models.shuttle import ShuttleCommand, ShuttleOperationalStatus
from models.wms import WMSCommandPayload
from services.services import COMMANDS_SENT_TOTAL, COMMAND_QUEUE_SIZE_METRIC
from services.shuttle_comms import send_command_to_shuttle

command_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=settings.COMMAND_QUEUE_MAX_SIZE)
shuttle_locks: Dict[str, asyncio.Lock] = {
    shuttle_id: asyncio.Lock() for shuttle_id in settings.SHUTTLES_CONFIG.keys()
}

async def process_wms_command_internal(shuttle_id: str, command: ShuttleCommand, params: Optional[str] = None) -> bool:
    shuttle_state = await get_shuttle_state_crud(shuttle_id)
    if not shuttle_state:
        logger.error(f"Шаттл {shuttle_id} не найден для команды {command.value}.")
        COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status="failure_not_found").inc()
        return False

    if command == ShuttleCommand.HOME:
        logger.info(f"Команда HOME для {shuttle_id}. Прерываем текущую операцию, если есть.")
        # Очищаем текущую команду, даже если шаттл в BUSY
        if shuttle_state.current_command:
            logger.debug(f"Очистка текущей команды {shuttle_state.current_command} перед HOME")
            await update_shuttle_state_crud(shuttle_id, {"current_command": None})
    elif command != ShuttleCommand.MRCD and command != ShuttleCommand.STATUS:
        if shuttle_state.status not in [ShuttleOperationalStatus.FREE, ShuttleOperationalStatus.UNKNOWN]:
            logger.warning(f"Шаттл {shuttle_id} в состоянии {shuttle_state.status}, команда {command.value} отклонена")
            COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status="failure_busy").inc()
            return False

    command_str_to_send = command.value
    param_for_shuttle = None
    if command in [ShuttleCommand.FIFO_NNN, ShuttleCommand.FILO_NNN]:
        if not params or not params.isdigit():
            logger.error(f"Некорректный параметр для {command.value}: {params}")
            COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status="failure_bad_params").inc()
            return False
        param_for_shuttle = params

    success = await send_command_to_shuttle(shuttle_id, command_str_to_send, param_for_shuttle)
    metric_status = "success" if success else "failure_send_error"
    COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status=metric_status).inc()

    if success:
        updates = {
            "last_message_received_from_wms": command.value + (f"-{params}" if params else ""),
            "last_seen": time.time()
        }
        if command != ShuttleCommand.MRCD:
            updates["current_command"] = command.value + (f"-{params}" if params else "")
        await update_shuttle_state_crud(shuttle_id, updates)
        logger.debug(f"Состояние шаттла {shuttle_id} обновлено после команды {command.value}")
    return success

async def command_processor_worker(worker_id: int):
    logger.info(f"Воркер обработки команд {worker_id} запущен.")
    while True:
        try:
            _priority, command_payload = await command_queue.get()
            COMMAND_QUEUE_SIZE_METRIC.set(command_queue.qsize())

            shuttle_id = command_payload.shuttle_id
            shuttle_lock = shuttle_locks.get(shuttle_id)

            if not shuttle_lock:
                logger.error(f"Не найден лок для шаттла {shuttle_id}. Команда {command_payload.command} пропущена.")
                command_queue.task_done()
                continue

            logger.info(f"Воркер {worker_id} взял команду для шаттла {shuttle_id}: {command_payload.command}")

            async with shuttle_lock:
                await process_wms_command_internal(
                    shuttle_id=command_payload.shuttle_id,
                    command=command_payload.command,
                    params=command_payload.params
                )

            command_queue.task_done()
        except asyncio.CancelledError:
            logger.info(f"Воркер обработки команд {worker_id} останавливается.")
            break
        except Exception as e:
            logger.error(f"Ошибка в воркере {worker_id}: {e}", exc_info=True)
            if not command_queue.empty() and '_priority' in locals():
                command_queue.task_done()

async def add_command_to_queue(payload: WMSCommandPayload, priority: int = 10):
    try:
        await command_queue.put((priority, payload))
        COMMAND_QUEUE_SIZE_METRIC.set(command_queue.qsize())
        logger.info(f"Команда {payload.command.value} для {payload.shuttle_id} добавлена в очередь с приоритетом {priority}.")
        return True
    except asyncio.QueueFull:
        logger.error(f"Очередь команд заполнена. Команда {payload.command.value} для {payload.shuttle_id} не добавлена.")
        return False