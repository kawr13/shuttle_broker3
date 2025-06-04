import asyncio
import time
import uuid
from typing import Dict, Optional, List

from icecream import ic

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import get_shuttle_state_crud, update_shuttle_state_crud
from models.shuttle import ShuttleCommand, ShuttleOperationalStatus
from services.services import COMMANDS_SENT_TOTAL, COMMAND_QUEUE_SIZE_METRIC
from services.shuttle_comms import send_command_to_shuttle

shuttle_locks: Dict[str, asyncio.Lock] = {}
shuttle_queues: Dict[str, asyncio.PriorityQueue] = {}
command_registry: Dict[str, Dict] = {}  # Реестр команд для отслеживания и отмены

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
                                       externaIID: Optional[str] = None, document_type: Optional[str] = None) -> bool:
    from services.retry_mechanism import retry_with_backoff
    
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

    # Используем механизм повторных попыток с экспоненциальной задержкой
    try:
        success = await retry_with_backoff(
            send_command_to_shuttle,
            shuttle_id, 
            command_str_to_send, 
            param_for_shuttle,
            max_retries=3,  # Максимальное количество повторных попыток
            base_delay=1.0,  # Начальная задержка в секундах
            max_delay=10.0   # Максимальная задержка в секундах
        )
        metric_status = "success" if success else "failure_send_error"
    except Exception as e:
        logger.error(f"Все попытки отправки команды {command.value} шаттлу {shuttle_id} не удались: {e}")
        success = False
        metric_status = "failure_all_retries_failed"
    
    COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value, status=metric_status).inc()

    if success:
        updates = {
            "last_message_received_from_wms": command.value + (f"-{params}" if params else ""),
            "last_seen": time.time(),
            "externaIID": externaIID,  # Сохраняем externaIID для последующего поиска
            "document_type": document_type  # Сохраняем тип документа
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
                              params: Optional[str] = None, externaIID: Optional[str] = None, 
                              priority: int = 10, document_type: Optional[str] = None):
    # Определение приоритета на основе типа команды
    command_priorities = {
        ShuttleCommand.HOME: 1,        # Наивысший приоритет
        ShuttleCommand.STATUS: 2,
        ShuttleCommand.BATTERY: 3,
        ShuttleCommand.MRCD: 4,
        ShuttleCommand.PALLET_OUT: 5,  # Выгрузка важнее загрузки
        ShuttleCommand.PALLET_IN: 6,
        ShuttleCommand.STACK_OUT: 7,
        ShuttleCommand.STACK_IN: 8,
        ShuttleCommand.FIFO_NNN: 9,
        ShuttleCommand.FILO_NNN: 10,
        ShuttleCommand.COUNT: 11,
        ShuttleCommand.WDH: 12,
        ShuttleCommand.WLH: 13
    }
    
    # Используем приоритет из словаря или переданный параметр (если он ниже)
    command_priority = command_priorities.get(command, 10)
    final_priority = min(command_priority, priority)
    
    if command in ALLOWED_WHEN_BUSY_COMMANDS:
        # Немедленная обработка для команд, разрешённых при BUSY
        async with shuttle_locks.get(shuttle_id, asyncio.Lock()):
            success = await process_wms_command_internal(
                shuttle_id=shuttle_id,
                command=command,
                params=params,
                externaIID=externaIID,
                document_type=document_type
            )
        if success:
            logger.info(f"Команда {command.value} для {shuttle_id} обработана немедленно.")
        else:
            logger.error(f"Не удалось обработать команду {command.value} для {shuttle_id}.")
        return success

    # Генерируем уникальный ID команды для возможности отмены
    command_id = f"{shuttle_id}_{command.value}_{int(time.time()*1000)}"
    
    # Обычные команды добавляются в приоритетную очередь
    data = (
        final_priority,  # Приоритет (меньше = выше)
        {
            "id": command_id,
            "shuttle_id": shuttle_id,
            "command": command,
            "params": params,
            "externaIID": externaIID,
            "document_type": document_type,  # Сохраняем тип документа
            "timestamp": time.time()
        }
    )
    try:
        await shuttle_queues[shuttle_id].put(data)
        COMMAND_QUEUE_SIZE_METRIC.set(sum(q.qsize() for q in shuttle_queues.values()))
        logger.info(f"Команда {command.value} для {shuttle_id} добавлена в очередь с приоритетом {final_priority}, ID: {command_id}")
        return command_id  # Возвращаем ID команды для возможности отмены
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
                    command_id = data.get("id")
                    
                    # Проверяем, не была ли команда отменена
                    if command_id in command_registry and command_registry[command_id].get("status") == "cancelled":
                        logger.info(f"Пропуск отмененной команды {command_id}")
                        shuttle_queues[shuttle_id].task_done()
                        continue
                    
                    # Обновляем статус команды в реестре
                    if command_id in command_registry:
                        command_registry[command_id]["status"] = "processing"
                    
                    async with shuttle_lock:
                        success = await process_wms_command_internal(
                            shuttle_id=shuttle_id,
                            command=data["command"],
                            params=data.get("params"),
                            externaIID=data.get("externaIID"),
                            document_type=data.get("document_type")
                        )
                        
                        # Обновляем статус команды в реестре
                        if command_id in command_registry:
                            command_registry[command_id]["status"] = "completed" if success else "failed"
                            command_registry[command_id]["completed_at"] = time.time()
                    
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
async def cancel_command(command_id: str) -> bool:
    """
    Отменяет команду, если она еще не выполнена.
    
    Args:
        command_id: ID команды для отмены
        
    Returns:
        True если команда была найдена и отменена, False в противном случае
    """
    if command_id not in command_registry:
        logger.warning(f"Команда с ID {command_id} не найдена для отмены")
        return False
    
    command_info = command_registry[command_id]
    shuttle_id = command_info["shuttle_id"]
    
    # Проверяем, что команда еще в очереди и не выполняется
    if command_info.get("status") == "queued":
        # Создаем временную очередь для перемещения команд
        temp_queue = asyncio.PriorityQueue()
        
        # Блокируем доступ к очереди шаттла
        async with shuttle_locks.get(shuttle_id, asyncio.Lock()):
            # Перемещаем все команды, кроме отменяемой, во временную очередь
            while not shuttle_queues[shuttle_id].empty():
                try:
                    priority, data = shuttle_queues[shuttle_id].get_nowait()
                    if data.get("id") != command_id:
                        await temp_queue.put((priority, data))
                except asyncio.QueueEmpty:
                    break
            
            # Возвращаем команды обратно в очередь шаттла
            while not temp_queue.empty():
                try:
                    item = temp_queue.get_nowait()
                    await shuttle_queues[shuttle_id].put(item)
                except asyncio.QueueEmpty:
                    break
        
        # Обновляем статус команды в реестре
        command_registry[command_id]["status"] = "cancelled"
        logger.info(f"Команда {command_id} для шаттла {shuttle_id} отменена")
        
        # Обновляем метрику размера очереди
        COMMAND_QUEUE_SIZE_METRIC.set(sum(q.qsize() for q in shuttle_queues.values()))
        return True
    else:
        logger.warning(f"Команда {command_id} для шаттла {shuttle_id} не может быть отменена, т.к. уже выполняется или завершена")
        return False