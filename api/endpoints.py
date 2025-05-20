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
    ShuttleCommand.HOME, # <-- Add HOME here
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
                raise HTTPException(status_code=503, detail=f"Нет подходящих шаттлов для склада {stock_name}" + (f", ячейка {cell_id}" if cell_id else ""))

            if shuttle_id not in settings.SHUTTLES_CONFIG:
                logger.error(f"Shuttle not found: {shuttle_id}")
                raise HTTPException(status_code=404, detail=f"Shuttle {shuttle_id} not found")

            success = await add_command_to_queue(
                shuttle_id=ic(shuttle_id),
                command=command,
                params=line.params,
                externaIID=externaIID,
                priority=5 if command == ShuttleCommand.HOME else 10
            )
            if not success:
                logger.error(f"Failed to queue command {command.value} for {shuttle_id}: Queue full")
                COMMANDS_SENT_TOTAL.labels(shuttle_id=shuttle_id, command_type=command.value,
                                           status="failure_queue_full").inc()
                raise HTTPException(status_code=503, detail="Command queue is full")
            queued_commands.append({"shuttle_id": shuttle_id, "command": command.value})

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