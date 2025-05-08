import asyncio
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

@router.post("/command", summary="Send a command to a shuttle")
async def send_command(payload: WMSCommandPayload):
    """
    Send a command to a shuttle.
    - **shuttle_id**: ID of the shuttle (e.g., virtual_shuttle_1)
    - **command**: Command to send (e.g., PALLET_IN, HOME)
    - **params**: Optional parameters (e.g., for FIFO-NNN)
    """
    if payload.shuttle_id not in settings.SHUTTLES_CONFIG:
        logger.error(f"Invalid shuttle_id: {payload.shuttle_id}")
        COMMANDS_SENT_TOTAL.labels(shuttle_id=payload.shuttle_id, command_type=payload.command.value, status="failure_not_found").inc()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Shuttle {payload.shuttle_id} not found")

    success = await add_command_to_queue(payload, priority=5 if payload.command == "HOME" else 10)
    if not success:
        logger.error(f"Failed to queue command {payload.command.value} for {payload.shuttle_id}: Queue full")
        COMMANDS_SENT_TOTAL.labels(shuttle_id=payload.shuttle_id, command_type=payload.command.value, status="failure_queue_full").inc()
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Command queue is full")

    logger.info(f"Command {payload.command.value} queued for {payload.shuttle_id}")
    return {"status": "queued", "shuttle_id": payload.shuttle_id, "command": payload.command.value, "params": payload.params}


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