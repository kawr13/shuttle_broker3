from pprint import pprint
from typing import Dict, Optional

import yaml
from fastapi import APIRouter, HTTPException, status
from icecream import ic

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import get_shuttle_state_crud
from models.shuttle import ShuttleCommand, ShuttleOperationalStatus
from models.wms import WMSCommandPayload
from services.command_processor import add_command_to_queue
from services.services import COMMANDS_SENT_TOTAL

router = APIRouter()

ALLOWED_WHEN_BUSY_COMMANDS = {
    ShuttleCommand.MRCD,
    ShuttleCommand.STATUS,
    ShuttleCommand.HOME,  # <-- Add HOME here
}


# Функция для поиска свободного шаттла
async def get_free_shuttle(stock_name: str, cell_id: Optional[str], command: str,
                           externaIID: Optional[str] = None) -> str | None:
    if command == ShuttleCommand.HOME.value and externaIID:
        # Поиск шаттла по externaIID
        for sid in settings.SHUTTLES_CONFIG.keys():
            state = await get_shuttle_state_crud(sid)
            if state and state.externaIID == externaIID:
                return sid
        logger.error(f"Шаттл с externaIID {externaIID} не найден")
        return None

    # Получаем шаттлы для склада
    shuttles = settings.STOCK_TO_SHUTTLE.get(stock_name, [])

    # Если ячейки не используются, shuttles уже является списком
    # Если ячейки будут добавлены позже, можно будет раскомментировать следующую логику:
    # if cell_id and isinstance(shuttles, dict) and cell_id in shuttles:
    #     shuttles = shuttles[cell_id]
    # else:
    #     shuttles = [sid for sublist in shuttles.values() for sid in sublist] if isinstance(shuttles, dict) else shuttles

    for sid in shuttles:
        state = await get_shuttle_state_crud(sid)
        if command in ALLOWED_WHEN_BUSY_COMMANDS:
            return sid
        if state and state.status == ShuttleOperationalStatus.FREE:
            return sid
    return None


@router.post("/command", summary="Send commands to shuttles")
async def send_command(payload: WMSCommandPayload):
    queued_commands = []
    for placement in payload.placement:
        stock_name = placement.nameStockERP
        for line in placement.placementLine:
            command = line.ShuttleIN
            cell_id = line.cell_id  # Может быть None
            externaIID = line.externaIID
            ic(command)
            shuttle_id = await get_free_shuttle(stock_name, cell_id, command.value, externaIID)
            if not shuttle_id:
                raise HTTPException(status_code=503, detail=f"Нет подходящих шаттлов для склада {stock_name}" + (
                    f", ячейка {cell_id}" if cell_id else ""))

            if shuttle_id not in settings.SHUTTLES_CONFIG:
                logger.error(f"Shuttle not found: {shuttle_id}")
                raise HTTPException(status_code=404, detail=f"Shuttle {shuttle_id} not found")

            result = await add_command_to_queue(
                shuttle_id=ic(shuttle_id),
                command=command,
                params=line.params,
                externaIID=externaIID,
                priority=5 if command == ShuttleCommand.HOME else 10
            )
            if not result:
                logger.error(f"Failed to queue command {command.value} for {shuttle_id}: Queue full")
                COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value,
                                           status="failure_queue_full").inc()
                raise HTTPException(status_code=503, detail="Command queue is full")

            # Сохраняем ID команды для возможности отмены
            command_id = result if isinstance(result, str) else None
            queued_commands.append({
                "shuttle_id": shuttle_id,
                "command": command.value,
                "command_id": command_id
            })

    logger.info(f"Queued commands: {queued_commands}")
    return {"status": "queued", "commands": queued_commands}


@router.get("/shuttle/{shuttle_id}/status", summary="Get status of a specific shuttle")
async def get_status(shuttle_id: str) -> Dict[str, Optional[str]]:
    """
    Get status of a specific shuttle.
    - **shuttle_id**: ID of the shuttle (e.g., virtual_shuttle_1)
    """
    if shuttle_id not in settings.SHUTTLES_CONFIG:
        logger.error(f"Invalid shuttle_id: {shuttle_id}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Shuttle {shuttle_id} not found")
    shuttle_state = await get_shuttle_state_crud(shuttle_id)
    response_model = shuttle_state.model_dump(mode="json")
    response = {
        **response_model,
        "wlh_hours": str(response_model["wlh_hours"]),
        "last_seen": str(response_model["last_seen"]),
    }
    pprint(response_model)
    if shuttle_state is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Shuttle {shuttle_id} not found")
    return response


@router.post("/shuttle/{shuttle_id}/move-to-stock", summary="Move shuttle to a new stock")
async def move_shuttle_to_stock(shuttle_id: str, new_stock: str):
    """
    Move a shuttle to a new stock and update configuration in Redis.
    - **shuttle_id**: ID of the shuttle (e.g., virtual_shuttle_1)
    - **new_stock**: Name of the new stock (e.g., "Второй склад")
    """
    if shuttle_id not in settings.SHUTTLES_CONFIG:
        raise HTTPException(status_code=404, detail=f"Shuttle {shuttle_id} not found")
    await settings.update_shuttle_stock(shuttle_id, new_stock)
    logger.info(f"Shuttle {shuttle_id} moved to stock {new_stock}")

    with open("initial_config.yaml", "r") as f:
        config_data = yaml.safe_load(f)
    for stock, shuttles in config_data["stock_to_shuttle"].items():
        if shuttle_id in shuttles:
            shuttles.remove(shuttle_id)
            break
    if new_stock not in config_data["stock_to_shuttle"]:
        config_data["stock_to_shuttle"][new_stock] = []
    config_data["stock_to_shuttle"][new_stock].append(shuttle_id)
    with open("initial_config.yaml", "w") as f:
        yaml.dump(config_data, f)
    return {"status": "success", "shuttle_id": shuttle_id, "new_stock": new_stock}


@router.delete("/command/{command_id}", summary="Cancel a queued command")
async def cancel_command_endpoint(command_id: str):
    """
    Отменяет команду, если она еще не выполнена.
    
    Args:
        command_id: ID команды для отмены
    
    Returns:
        Статус операции отмены
    """
    from services.command_processor import cancel_command

    success = await cancel_command(command_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Команда {command_id} не найдена или не может быть отменена")

    return {"status": "cancelled", "command_id": command_id}


@router.get("/wms/integration/status", summary="Get WMS integration status")
async def get_wms_integration_status():
    """
    Получает статус интеграции с WMS API.
    """
    from services.wms_integration import wms_integration

    status = {
        "enabled": settings.WMS_INTEGRATION_ENABLED,
        "running": wms_integration.running if settings.WMS_INTEGRATION_ENABLED else False,
        "last_poll_time": wms_integration.last_poll_time.isoformat() if settings.WMS_INTEGRATION_ENABLED else None,
        "poll_interval": settings.WMS_POLL_INTERVAL,
        "processed_commands_count": len(wms_integration.processed_commands) if settings.WMS_INTEGRATION_ENABLED else 0
    }

    return status


@router.post("/wms/integration/start", summary="Start WMS integration")
async def start_wms_integration():
    """
    Запускает интеграцию с WMS API.
    """
    if not settings.WMS_INTEGRATION_ENABLED:
        raise HTTPException(status_code=400, detail="WMS integration is disabled in settings")

    from services.wms_integration import wms_integration

    if wms_integration.running:
        return {"status": "already_running"}

    await wms_integration.start()
    return {"status": "started"}


@router.post("/wms/integration/stop", summary="Stop WMS integration")
async def stop_wms_integration():
    """
    Останавливает интеграцию с WMS API.
    """
    if not settings.WMS_INTEGRATION_ENABLED:
        raise HTTPException(status_code=400, detail="WMS integration is disabled in settings")

    from services.wms_integration import wms_integration

    if not wms_integration.running:
        return {"status": "not_running"}

    await wms_integration.stop()
    return {"status": "stopped"}



@router.post("/wms-mock/test-command", summary="Create test command for WMS integration")
async def create_test_command(command_type: str = "shipment", shuttle_command: str = "PALLET_IN", stock_name: str = "Главный", cell_id: str = "", params: str = ""):
    """
    Создает тестовую команду для проверки интеграции с WMS.
    
    Args:
        command_type: Тип команды (shipment или transfer)
        shuttle_command: Команда для шаттла (PALLET_IN, PALLET_OUT, FIFO, FILO, HOME и т.д.)
        stock_name: Название склада
        cell_id: ID ячейки (опционально)
        params: Параметры команды (опционально)
    
    Returns:
        Информация о созданной команде
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:8000/wms-mock/create-command",
            params={
                "command_type": command_type,
                "shuttle_command": shuttle_command,
                "stock_name": stock_name,
                "cell_id": cell_id,
                "params": params
            }
        ) as response:
            if response.status == 200:
                result = await response.json()
                logger.info(f"Создана тестовая команда: {result}")
                return {
                    "status": "success",
                    "message": "Тестовая команда создана",
                    "command": {
                        "type": command_type,
                        "shuttle_command": shuttle_command,
                        "stock_name": stock_name,
                        "cell_id": cell_id,
                        "params": params,
                        "external_id": result.get("external_id")
                    }
                }
            else:
                error_text = await response.text()
                logger.error(f"Ошибка при создании тестовой команды: {response.status}, {error_text}")
                raise HTTPException(status_code=500, detail=f"Ошибка при создании тестовой команды: {error_text}")

@router.post("/wms-integration/restart", summary="Restart WMS integration")
async def restart_wms_integration():
    """
    Перезапускает интеграцию с WMS API.
    """
    if not settings.WMS_INTEGRATION_ENABLED:
        raise HTTPException(status_code=400, detail="WMS integration is disabled in settings")
    
    from services.wms_integration import wms_integration
    
    # Останавливаем интеграцию
    await wms_integration.stop()
    logger.info("WMS интеграция остановлена для перезапуска")
    
    # Запускаем интеграцию заново
    await wms_integration.start()
    logger.info("WMS интеграция перезапущена")
    
    return {"status": "restarted"}