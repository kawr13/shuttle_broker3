from pprint import pprint
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, status

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import get_shuttle_state_crud
from models.wms import WMSCommandPayload
from services.command_processor import add_command_to_queue
from services.services import COMMANDS_SENT_TOTAL

router = APIRouter()

# Маппинг склада (nameStockERP) на shuttle_id
STOCK_TO_SHUTTLE = {
    "Главный": "virtual_shuttle_1",
    # Добавьте другие маппинги по необходимости
}


@router.post("/command", summary="Send commands to shuttles")
async def send_command(payload: WMSCommandPayload):
    """
    Send commands to shuttles based on WMS placement data.
    """
    queued_commands = []
    for placement in payload.placement:
        # Определяем shuttle_id по nameStockERP
        shuttle_id = STOCK_TO_SHUTTLE.get(placement.nameStockERP)
        if not shuttle_id:
            logger.error(f"Unknown stock: {placement.nameStockERP}")
            raise HTTPException(status_code=400, detail=f"Unknown stock: {placement.nameStockERP}")
        if shuttle_id not in settings.SHUTTLES_CONFIG:
            logger.error(f"Shuttle not found: {shuttle_id}")
            raise HTTPException(status_code=404, detail=f"Shuttle {shuttle_id} not found")

        # Обрабатываем каждую строку размещения
        for line in placement.placementLine:
            command = line.ShuttleIN
            success = await add_command_to_queue(
                shuttle_id=shuttle_id,
                command=command,
                params=line.params,  # Используем params, если они есть
                externaIID=line.externaIID,
                priority=5 if command == "HOME" else 10
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