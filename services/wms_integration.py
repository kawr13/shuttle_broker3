import asyncio
import base64
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

import aiohttp
from pydantic import BaseModel

from core.config import settings
from core.logging_config import logger
from models.shuttle import ShuttleCommand
from services.command_processor import add_command_to_queue


class WMSIntegration:
    def __init__(self):
        self.base_url = settings.WMS_API_URL
        self.username = settings.WMS_API_USERNAME
        self.password = settings.WMS_API_PASSWORD
        self.poll_interval = settings.WMS_POLL_INTERVAL  # в секундах
        self.last_poll_time = datetime.now() - timedelta(minutes=30)  # Начинаем с получения команд за последние 30 минут
        self.running = False
        self.task = None
        
        # Маппинг команд WMS на команды шаттлов
        self.command_mapping = {
            "PALLET_IN": ShuttleCommand.PALLET_IN,
            "PALLET_OUT": ShuttleCommand.PALLET_OUT,
            "FIFO": ShuttleCommand.FIFO_NNN,
            "FILO": ShuttleCommand.FILO_NNN,
            "STACK_IN": ShuttleCommand.STACK_IN,
            "STACK_OUT": ShuttleCommand.STACK_OUT,
            "HOME": ShuttleCommand.HOME,
            "COUNT": ShuttleCommand.COUNT,
            "STATUS": ShuttleCommand.STATUS
        }
        
        # Кэш для отслеживания обработанных команд
        self.processed_commands = set()
    
    async def start(self):
        """Запускает процесс интеграции с WMS"""
        if self.running:
            return
        
        self.running = True
        self.task = asyncio.create_task(self._poll_loop())
        logger.info("Запущена интеграция с WMS")
    
    async def stop(self):
        """Останавливает процесс интеграции с WMS"""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("Интеграция с WMS остановлена")
    
    async def _poll_loop(self):
        """Основной цикл опроса WMS"""
        logger.info("Запущен цикл опроса WMS API")
        while self.running:
            try:
                logger.info(f"Опрашиваем WMS API ({self.base_url})...")
                
                # Получаем новые команды
                await self._fetch_and_process_commands()
                
                # Обновляем статусы выполненных команд
                await self._update_command_statuses()
                
                # Обновляем время последнего опроса
                self.last_poll_time = datetime.now()
                
                logger.info(f"Опрос WMS API завершен, следующий опрос через {self.poll_interval} секунд")
                
                # Ждем до следующего опроса
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                logger.info("Цикл опроса WMS API отменен")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле опроса WMS: {e}", exc_info=True)
                await asyncio.sleep(10)  # Короткая пауза перед повторной попыткой
    
    async def _fetch_and_process_commands(self):
        """Получает и обрабатывает команды из WMS"""
        try:
            # Получаем команды из разных источников
            logger.info("Получаем команды из отгрузок...")
            shipment_commands = await self._get_shipment_commands()
            logger.info(f"Получено {len(shipment_commands)} команд из отгрузок")
            
            logger.info("Получаем команды из перемещений...")
            transfer_commands = await self._get_transfer_commands()
            logger.info(f"Получено {len(transfer_commands)} команд из перемещений")
            
            # Обрабатываем полученные команды
            if shipment_commands:
                logger.info(f"Обрабатываем {len(shipment_commands)} команд из отгрузок")
                await self._process_commands(shipment_commands, "shipment")
            
            if transfer_commands:
                logger.info(f"Обрабатываем {len(transfer_commands)} команд из перемещений")
                await self._process_commands(transfer_commands, "transfer")
            
        except Exception as e:
            logger.error(f"Ошибка при получении команд из WMS: {e}", exc_info=True)
    
    async def _get_shipment_commands(self) -> List[Dict]:
        """Получает команды из отгрузок"""
        start_time = self.last_poll_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        url = f"{self.base_url}/exec?action=IncomeApi.getShipmentStatusesPeriod&p={start_time}&p={end_time}"
        
        auth_header = self._get_auth_header()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=auth_header) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("shipment", [])
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка при получении отгрузок: {response.status}, {error_text}")
                    return []
    
    async def _get_transfer_commands(self) -> List[Dict]:
        """Получает команды из перемещений"""
        start_time = self.last_poll_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        url = f"{self.base_url}/exec?action=IncomeApi.getTransferStatusesPeriod&p={start_time}&p={end_time}"
        
        auth_header = self._get_auth_header()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=auth_header) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("transfer", [])
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка при получении перемещений: {response.status}, {error_text}")
                    return []
    
    async def _process_commands(self, commands: List[Dict], command_type: str):
        """Обрабатывает полученные команды"""
        for command in commands:
            external_id = command.get("externalId")
            
            # Проверяем, не обрабатывали ли мы уже эту команду
            if external_id in self.processed_commands:
                continue
            
            # Получаем детали команды с использованием механизма повторных попыток
            from services.retry_utils import retry_async
            try:
                command_details = await retry_async(
                    self._get_command_details,
                    external_id, 
                    command_type,
                    endpoint=f"getObject_{command_type}",
                    max_retries=3
                )
                if not command_details:
                    continue
            except Exception as e:
                logger.error(f"Не удалось получить детали команды {external_id}: {e}")
                continue
            
            # Извлекаем строки команды
            lines = []
            if command_type == "shipment":
                lines = command_details.get("shipmentLine", [])
            elif command_type == "transfer":
                lines = command_details.get("transferLine", [])
            
            # Обрабатываем каждую строку как отдельную команду для шаттла
            for line in lines:
                line_external_id = line.get("externalId")
                shuttle_command = line.get("shuttleCommand")
                
                if not shuttle_command or shuttle_command not in self.command_mapping:
                    continue
                
                # Определяем склад и ячейку
                stock_name = command_details.get("warehouse", "")
                cell_id = line.get("cell", "")
                
                # Определяем параметры команды
                params = line.get("params", "")
                
                # Определяем команду шаттла
                shuttle_cmd = self.command_mapping[shuttle_command]
                
                # Добавляем команду в очередь
                from api.endpoints import get_free_shuttle
                shuttle_id = await get_free_shuttle(stock_name, cell_id, shuttle_cmd.value, line_external_id)
                
                if shuttle_id:
                    result = await add_command_to_queue(
                        shuttle_id=shuttle_id,
                        command=shuttle_cmd,
                        params=params,
                        externaIID=line_external_id,
                        priority=5 if shuttle_cmd == ShuttleCommand.HOME else 10,
                        document_type=command_type  # Сохраняем тип документа
                    )
                    
                    if result:
                        logger.info(f"Команда {shuttle_cmd.value} для шаттла {shuttle_id} добавлена в очередь (WMS externalId: {line_external_id}, тип: {command_type})")
                        # Добавляем в список обработанных
                        self.processed_commands.add(line_external_id)
                        
                        # Обновляем метрики
                        from services.services import WMS_COMMANDS_PROCESSED
                        WMS_COMMANDS_PROCESSED.labels(
                            command_type=shuttle_cmd.value,
                            document_type=command_type,
                            status="queued"
                        ).inc()
                    else:
                        logger.error(f"Не удалось добавить команду {shuttle_cmd.value} в очередь для шаттла {shuttle_id}")
                        from services.services import WMS_COMMANDS_PROCESSED
                        WMS_COMMANDS_PROCESSED.labels(
                            command_type=shuttle_cmd.value,
                            document_type=command_type,
                            status="failed"
                        ).inc()
                else:
                    logger.warning(f"Не найден свободный шаттл для команды {shuttle_cmd.value} на складе {stock_name}, ячейка {cell_id}")
                    from services.services import WMS_COMMANDS_PROCESSED
                    WMS_COMMANDS_PROCESSED.labels(
                        command_type=shuttle_cmd.value,
                        document_type=command_type,
                        status="no_shuttle"
                    ).inc()
    
    async def _get_command_details(self, external_id: str, command_type: str) -> Optional[Dict]:
        """Получает детали команды по её ID"""
        action = ""
        if command_type == "shipment":
            action = "IncomeApi.getObject&p=shipment"
        elif command_type == "transfer":
            action = "IncomeApi.getObject&p=transfer"
        
        url = f"{self.base_url}/exec?action={action}&p={external_id}"
        
        auth_header = self._get_auth_header()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=auth_header, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if command_type == "shipment":
                            return data.get("shipment", [{}])[0]
                        elif command_type == "transfer":
                            return data.get("transfer", [{}])[0]
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка при получении деталей команды {external_id}: {response.status}, {error_text}")
                        from services.services import WMS_API_ERRORS
                        WMS_API_ERRORS.labels(
                            endpoint=f"getObject_{command_type}",
                            error_type=f"http_{response.status}"
                        ).inc()
                        return None
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при получении деталей команды {external_id}")
            from services.services import WMS_API_ERRORS
            WMS_API_ERRORS.labels(
                endpoint=f"getObject_{command_type}",
                error_type="timeout"
            ).inc()
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении деталей команды {external_id}: {e}")
            from services.services import WMS_API_ERRORS
            WMS_API_ERRORS.labels(
                endpoint=f"getObject_{command_type}",
                error_type="exception"
            ).inc()
            return None
    
    async def _update_command_statuses(self):
        """Обновляет статусы выполненных команд в WMS"""
        # Получаем список выполненных команд
        from services.command_processor import command_registry
        
        completed_commands = []
        for cmd_id, cmd_info in command_registry.items():
            if cmd_info.get("status") == "completed" and cmd_info.get("externaIID") and not cmd_info.get("wms_updated"):
                completed_commands.append(cmd_info)
        
        # Обновляем статусы в WMS
        for cmd in completed_commands:
            external_id = cmd.get("externaIID")
            shuttle_id = cmd.get("shuttle_id")
            command = cmd.get("command")
            
            # Используем сохраненный тип документа или по умолчанию "shipment"
            document_type = cmd.get("document_type", "shipment")
            
            # Используем механизм повторных попыток
            from services.retry_utils import retry_async
            try:
                success = await retry_async(
                    self._update_status_in_wms,
                    external_id, 
                    document_type, 
                    "done",
                    endpoint=f"updateStatus_{document_type}",
                    max_retries=3
                )
                
                if success:
                    logger.info(f"Статус команды {command.value} для шаттла {shuttle_id} обновлен в WMS (externalId: {external_id}, тип: {document_type})")
                    # Помечаем команду как обновленную в WMS
                    command_registry[cmd_id]["wms_updated"] = True
                    
                    # Обновляем метрики
                    from services.services import WMS_STATUS_UPDATES
                    WMS_STATUS_UPDATES.labels(
                        command_type=command.value,
                        document_type=document_type,
                        status="success"
                    ).inc()
                else:
                    logger.error(f"Не удалось обновить статус команды {command.value} для шаттла {shuttle_id} в WMS (externalId: {external_id}, тип: {document_type})")
                    from services.services import WMS_STATUS_UPDATES
                    WMS_STATUS_UPDATES.labels(
                        command_type=command.value,
                        document_type=document_type,
                        status="failure"
                    ).inc()
            except Exception as e:
                logger.error(f"Ошибка при обновлении статуса команды {command.value} для шаттла {shuttle_id} в WMS: {e}")
                from services.services import WMS_API_ERRORS
                WMS_API_ERRORS.labels(
                    endpoint=f"updateStatus_{document_type}",
                    error_type="exception"
                ).inc()
    
    async def _update_status_in_wms(self, external_id: str, document_type: str, status: str) -> bool:
        """Обновляет статус команды в WMS"""
        url = f"{self.base_url}/exec?action=IncomeApi.insertUpdate"
        
        # Формируем тело запроса в зависимости от типа документа
        body = {}
        if document_type == "shipment":
            body = {
                "shipment": [
                    {
                        "externalId": external_id,
                        "shipmentLine": [
                            {
                                "externalId": external_id,
                                "quantityShipped": 1,  # Предполагаем, что количество = 1
                                "status": status
                            }
                        ]
                    }
                ]
            }
        elif document_type == "transfer":
            body = {
                "transfer": [
                    {
                        "externalId": external_id,
                        "transferLine": [
                            {
                                "externalId": external_id,
                                "quantityTransferred": 1,  # Предполагаем, что количество = 1
                                "status": status
                            }
                        ]
                    }
                ]
            }
        
        auth_header = self._get_auth_header()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=auth_header, json=body, timeout=10) as response:
                    if response.status == 200:
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка при обновлении статуса в WMS: {response.status}, {error_text}")
                        from services.services import WMS_API_ERRORS
                        WMS_API_ERRORS.labels(
                            endpoint=f"updateStatus_{document_type}",
                            error_type=f"http_{response.status}"
                        ).inc()
                        return False
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при обновлении статуса в WMS для {external_id}")
            from services.services import WMS_API_ERRORS
            WMS_API_ERRORS.labels(
                endpoint=f"updateStatus_{document_type}",
                error_type="timeout"
            ).inc()
            return False
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса в WMS для {external_id}: {e}")
            from services.services import WMS_API_ERRORS
            WMS_API_ERRORS.labels(
                endpoint=f"updateStatus_{document_type}",
                error_type="exception"
            ).inc()
            return False
    
    def _get_auth_header(self) -> Dict[str, str]:
        """Формирует заголовок авторизации"""
        auth_string = f"{self.username}:{self.password}"
        auth_bytes = auth_string.encode('ascii')
        base64_bytes = base64.b64encode(auth_bytes)
        base64_auth = base64_bytes.decode('ascii')
        
        return {
            "Authorization": f"Basic {base64_auth}",
            "Content-Type": "application/json"
        }


# Функция для патчинга WMS интеграции
def use_wms_mock():
    from services.wms_integration import wms_integration
    
    # Переопределяем URL для API
    wms_integration.base_url = "http://localhost:8000/wms-mock"
    
    # Переопределяем методы получения команд
    original_get_shipment_commands = wms_integration._get_shipment_commands
    
    async def mock_get_shipment_commands():
        url = f"{wms_integration.base_url}/shipment-statuses"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("shipment", [])
                return []
    
    wms_integration._get_shipment_commands = mock_get_shipment_commands
    
    # Аналогично переопределить другие методы...
    
    return "WMS интеграция настроена на использование мок-API"



# Создаем глобальный экземпляр интеграции с WMS
wms_integration = WMSIntegration()