import asyncio
import json
import random
import time
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException

from core.logging_config import logger

# Создаем роутер для мок-API
mock_router = APIRouter(prefix="/wms-mock", tags=["WMS Mock"])

# Хранилище для мок-данных
mock_data = {
    "shipments": {},
    "transfers": {},
    "processed_commands": set()
}

# Генератор уникальных ID
def generate_id():
    return f"mock-{int(time.time())}-{random.randint(1000, 9999)}"

@mock_router.post("/create-command")
async def create_mock_command(command_type: str, shuttle_command: str, stock_name: str, cell_id: str = "", params: str = ""):
    """Создает мок-команду для тестирования интеграции с WMS"""
    external_id = generate_id()
    
    command_data = {
        "external_id": external_id,
        "shuttle_command": shuttle_command,
        "stock_name": stock_name,
        "cell_id": cell_id,
        "params": params,
        "status": "new",
        "created_at": datetime.now().isoformat()
    }
    
    if command_type == "shipment":
        mock_data["shipments"][external_id] = command_data
    else:
        mock_data["transfers"][external_id] = command_data
    
    logger.info(f"Создана мок-команда: {command_type} {shuttle_command} для склада {stock_name}, ID: {external_id}")
    return {"external_id": external_id, "status": "created"}

@mock_router.get("/exec")
async def exec_api(action: str, p: list[str] = None):
    """Обрабатывает запросы к API WMS"""
    logger.info(f"Получен запрос к WMS API: action={action}, p={p}")
    
    if action == "IncomeApi.getShipmentStatusesPeriod":
        return await get_shipment_statuses()
    elif action == "IncomeApi.getTransferStatusesPeriod":
        return await get_transfer_statuses()
    elif action == "IncomeApi.getObject":
        if p and len(p) >= 2:
            object_type = p[0]
            object_id = p[1]
            
            if object_type == "shipment":
                return await get_shipment_details(object_id)
            elif object_type == "transfer":
                return await get_transfer_details(object_id)
            else:
                raise HTTPException(status_code=400, detail=f"Неверный тип объекта: {object_type}")
        else:
            raise HTTPException(status_code=400, detail="Недостаточно параметров для getObject")
    elif action == "IncomeApi.insertUpdate":
        return {"status": "success"}
    else:
        raise HTTPException(status_code=400, detail=f"Неизвестное действие: {action}")


@mock_router.get("/shipment-statuses")
async def get_shipment_statuses():
    """Возвращает статусы отгрузок для тестирования интеграции с WMS"""
    shipments = []
    
    for ext_id, data in mock_data["shipments"].items():
        if data["status"] == "new":  # Возвращаем только новые команды
            shipment = {
                "externalId": ext_id,
                "nameStatus": "New",
                "idStatus": "new"
            }
            shipments.append(shipment)
    
    logger.info(f"Возвращаем {len(shipments)} отгрузок")
    return {"shipment": shipments}

@mock_router.get("/transfer-statuses")
async def get_transfer_statuses():
    """Возвращает статусы перемещений для тестирования интеграции с WMS"""
    transfers = []
    
    for ext_id, data in mock_data["transfers"].items():
        if data["status"] == "new":  # Возвращаем только новые команды
            transfer = {
                "externalId": ext_id,
                "nameStatus": "New",
                "idStatus": "new"
            }
            transfers.append(transfer)
    
    logger.info(f"Возвращаем {len(transfers)} перемещений")
    return {"transfer": transfers}

@mock_router.get("/object/shipment/{external_id}")
async def get_shipment_details(external_id: str):
    """Возвращает детали отгрузки для тестирования интеграции с WMS"""
    if external_id not in mock_data["shipments"]:
        logger.warning(f"Отгрузка с ID {external_id} не найдена")
        return {"error": "Not found"}
    
    data = mock_data["shipments"][external_id]
    
    response = {
        "shipment": [{
            "externalId": external_id,
            "warehouse": data["stock_name"],
            "shipmentLine": [{
                "externalId": external_id,
                "shuttleCommand": data["shuttle_command"],
                "cell": data["cell_id"],
                "params": data["params"]
            }]
        }]
    }
    
    logger.info(f"Возвращаем детали отгрузки {external_id}: {response}")
    return response

@mock_router.get("/object/transfer/{external_id}")
async def get_transfer_details(external_id: str):
    """Возвращает детали перемещения для тестирования интеграции с WMS"""
    if external_id not in mock_data["transfers"]:
        logger.warning(f"Перемещение с ID {external_id} не найдено")
        return {"error": "Not found"}
    
    data = mock_data["transfers"][external_id]
    
    response = {
        "transfer": [{
            "externalId": external_id,
            "warehouse": data["stock_name"],
            "transferLine": [{
                "externalId": external_id,
                "shuttleCommand": data["shuttle_command"],
                "cell": data["cell_id"],
                "params": data["params"]
            }]
        }]
    }
    
    logger.info(f"Возвращаем детали перемещения {external_id}: {response}")
    return response

@mock_router.post("/update-status")
async def update_status(request: Request):
    """Принимает обновления статусов от шлюза"""
    body = await request.json()
    logger.info(f"Получено обновление статуса: {body}")
    
    # Обрабатываем обновление статуса для shipment
    if "shipment" in body:
        for shipment in body["shipment"]:
            ext_id = shipment.get("externalId")
            if ext_id in mock_data["shipments"]:
                for line in shipment.get("shipmentLine", []):
                    if line.get("status") == "done":
                        mock_data["shipments"][ext_id]["status"] = "done"
                        mock_data["processed_commands"].add(ext_id)
                        logger.info(f"Обновлен статус отгрузки {ext_id} на 'done'")
    
    # Обрабатываем обновление статуса для transfer
    if "transfer" in body:
        for transfer in body["transfer"]:
            ext_id = transfer.get("externalId")
            if ext_id in mock_data["transfers"]:
                for line in transfer.get("transferLine", []):
                    if line.get("status") == "done":
                        mock_data["transfers"][ext_id]["status"] = "done"
                        mock_data["processed_commands"].add(ext_id)
                        logger.info(f"Обновлен статус перемещения {ext_id} на 'done'")
    
    return {"status": "success"}

@mock_router.get("/status")
async def get_mock_status():
    """Возвращает текущий статус мок-сервера"""
    return {
        "shipments_count": len(mock_data["shipments"]),
        "transfers_count": len(mock_data["transfers"]),
        "processed_commands": len(mock_data["processed_commands"]),
        "active_commands": {
            "shipments": {k: v for k, v in mock_data["shipments"].items() if v["status"] == "new"},
            "transfers": {k: v for k, v in mock_data["transfers"].items() if v["status"] == "new"}
        }
    }

@mock_router.post("/enable")
async def enable_wms_mock():
    """Включает использование мок-сервера для WMS интеграции"""
    from services.wms_integration import wms_integration
    
    # Патчим методы в объекте wms_integration
    wms_integration.base_url = "http://localhost:8000/wms-mock"
    
    logger.info("WMS интеграция настроена на использование мок-сервера")
    return {"status": "success", "message": "WMS интеграция настроена на использование мок-сервера"}