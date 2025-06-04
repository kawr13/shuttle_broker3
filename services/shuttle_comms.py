import asyncio
import time
from typing import Optional

import aiohttp
from attrs import define

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import update_shuttle_state_crud, get_shuttle_state_crud
from models.shuttle import ShuttleOperationalStatus, ShuttleCommand
from services.services import ACTIVE_SHUTTLE_CONNECTIONS, SHUTTLE_BATTERY_LEVEL, SHUTTLE_MESSAGES_RECEIVED_TOTAL, \
    SHUTTLE_ERRORS_TOTAL


@define(slots=True, frozen=True)
class ConfigShuttle:
    SHUTTLE_TIMEOUT_SECONDS: int = 30  # Таймаут для зависших шаттлов
    WMS_WEBHOOK_URL: str = settings.WMS_WEBHOOK_URL


conf = ConfigShuttle()


async def send_command_to_shuttle(shuttle_id: str, command: str, params: Optional[str] = None) -> bool:
    shuttle_config = settings.SHUTTLES_CONFIG.get(shuttle_id)
    if not shuttle_config:
        logger.error(f"Конфигурация для шаттла {shuttle_id} не найдена.")
        return False

    full_command = command
    if params:
        full_command = f"{command}-{int(params):03}"

    if not full_command.endswith('\n'):
        full_command += '\n'

    logger.info(
        f"Отправка команды '{full_command.strip()}' шаттлу {shuttle_id} ({shuttle_config.host}:{shuttle_config.command_port})")

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(shuttle_config.host, shuttle_config.command_port),
            timeout=settings.TCP_CONNECT_TIMEOUT
        )

        writer.write(full_command.encode('utf-8'))
        await asyncio.wait_for(writer.drain(), timeout=settings.TCP_WRITE_TIMEOUT)
        logger.info(f"Команда '{full_command.strip()}' успешно отправлена шаттлу {shuttle_id}")

        writer.close()
        await writer.wait_closed()
        return True

    except asyncio.TimeoutError:
        logger.error(f"Таймаут при отправке команды шаттлу {shuttle_id} ({full_command.strip()})")
        await update_shuttle_state_crud(shuttle_id,
                                        {"status": ShuttleOperationalStatus.ERROR, "error_code": "TCP_TIMEOUT_SEND"})
    except ConnectionRefusedError:
        logger.error(f"Соединение отклонено шаттлом {shuttle_id} ({shuttle_config.host}:{shuttle_config.command_port})")
        await update_shuttle_state_crud(shuttle_id,
                                        {"status": ShuttleOperationalStatus.ERROR, "error_code": "CONNECTION_REFUSED"})
    except OSError as e:
        logger.error(f"Ошибка сети при отправке команды шаттлу {shuttle_id}: {e}")
        await update_shuttle_state_crud(shuttle_id, {"status": ShuttleOperationalStatus.ERROR,
                                                     "error_code": f"NET_ERROR_{e.errno}"})
    except Exception as e:
        logger.error(f"Неизвестная ошибка при отправке команды шаттлу {shuttle_id}: {e}")
        await update_shuttle_state_crud(shuttle_id,
                                        {"status": ShuttleOperationalStatus.ERROR, "error_code": "UNKNOWN_SEND_ERROR"})
    return False


async def send_to_wms_webhook(shuttle_id: str, message: str, status: str, error_code: Optional[str] = None,
                              externaIID: Optional[str] = None):
    if not conf.WMS_WEBHOOK_URL or not conf.WMS_WEBHOOK_URL.strip():
        logger.warning("WMS_WEBHOOK_URL не настроен или пуст. Пропуск отправки в WMS.")
        return

    # Улучшенная обработка externaIID
    from services.command_processor import command_registry
    
    # Если externaIID не передан, пытаемся найти его в реестре команд
    if not externaIID:
        # Ищем активную команду для этого шаттла
        for cmd_id, cmd_info in command_registry.items():
            if cmd_info.get("shuttle_id") == shuttle_id and cmd_info.get("status") in ["queued", "processing"]:
                externaIID = cmd_info.get("externaIID")
                logger.debug(f"Найден externaIID {externaIID} для шаттла {shuttle_id} в реестре команд")
                break

    payload = {
        "shuttle_id": shuttle_id,
        "message": message,
        "status": status,
        "error_code": error_code,
        "externaIID": externaIID,
        "timestamp": time.time()
    }
    logger.debug(f"Отправка Webhook в WMS: {payload}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(conf.WMS_WEBHOOK_URL, json=payload, timeout=10) as response:
                if response.status >= 200 and response.status < 300:  # Check for 2xx success codes
                    logger.info(f"Успешно отправлено в WMS для {shuttle_id}: {message}")
                    response_text = await response.text()
                    logger.debug(f"Ответ от WMS: {response_text}")
                    return True
                else:
                    logger.error(f"Ошибка отправки в WMS: {response.status} {await response.text()}")
                    return False
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при отправке webhook в WMS для {shuttle_id}")
        return False
    except Exception as e:
        logger.error(f"Ошибка при отправке Webhook в WMS: {e}, данные: {payload}")
        return False


async def process_shuttle_message_internal(shuttle_id: str, message: str):
    # Импортируем конечный автомат
    from services.state_machine import shuttle_state_machine
    
    # Fetch current state to get externaIID associated with the command
    # This externaIID was stored when the command was initially processed and sent.
    current_state = await get_shuttle_state_crud(shuttle_id)
    current_externaIID = current_state.externaIID if current_state else None

    updates = {"last_message_sent_to_wms": message, "last_seen": time.time()}
    
    # Определяем триггер для конечного автомата на основе сообщения
    trigger = None
    
    if message.endswith("_STARTED"):
        # Определяем тип операции на основе сообщения
        if "PALLET_IN" in message:
            trigger = ShuttleCommand.PALLET_IN
        elif "PALLET_OUT" in message:
            trigger = ShuttleCommand.PALLET_OUT
        elif "FIFO" in message:
            trigger = ShuttleCommand.FIFO_NNN
        elif "FILO" in message:
            trigger = ShuttleCommand.FILO_NNN
        elif "STACK_IN" in message:
            trigger = ShuttleCommand.STACK_IN
        elif "STACK_OUT" in message:
            trigger = ShuttleCommand.STACK_OUT
        elif "HOME" in message:
            trigger = ShuttleCommand.HOME
        else:
            # Если не удалось определить конкретную операцию, используем общее состояние BUSY
            updates["status"] = ShuttleOperationalStatus.BUSY
    elif message.endswith("_DONE"):
        trigger = "DONE"
    elif message.endswith("_ABORT"):
        updates["error_code"] = message
        trigger = "ERROR"

    if message.startswith("LOCATION="):
        updates["location_data"] = message.split("=", 1)[1]
        trigger = "DONE"  # Location implies task done
        updates["current_command"] = None
    elif message.startswith("COUNT_") and "=" in message:
        updates["pallet_count_data"] = message
        trigger = "DONE"  # Count implies task done
        updates["current_command"] = None
    elif message.startswith("STATUS="):
        status_val = message.split("=", 1)[1].upper()  # Ensure uppercase for matching
        status_map = {
            "FREE": ShuttleOperationalStatus.FREE,
            "CARGO": ShuttleOperationalStatus.BUSY,  # Map CARGO to BUSY
            "BUSY": ShuttleOperationalStatus.BUSY,
            "NOT_READY": ShuttleOperationalStatus.NOT_READY,
            "MOVING": ShuttleOperationalStatus.MOVING,
            "LOADING": ShuttleOperationalStatus.LOADING,
            "UNLOADING": ShuttleOperationalStatus.UNLOADING,
            "CHARGING": ShuttleOperationalStatus.CHARGING,
            "LOW_BATTERY": ShuttleOperationalStatus.LOW_BATTERY
        }
        updates["status"] = status_map.get(status_val, ShuttleOperationalStatus.UNKNOWN)
        # If STATUS message indicates FREE or NOT_READY, clear current command
        if updates["status"] in [ShuttleOperationalStatus.FREE, ShuttleOperationalStatus.NOT_READY,
                                 ShuttleOperationalStatus.UNKNOWN]:
            updates["current_command"] = None  # Clear command if status indicates not busy
    elif message.startswith("BATTERY="):
        level_str = message.split("=", 1)[1]
        updates["battery_level"] = level_str
        try:
            # Handle '<' prefix if present
            parsed_level = float(level_str.replace('%', '').lstrip('<'))
            SHUTTLE_BATTERY_LEVEL.labels(shuttle_id=shuttle_id).set(parsed_level)
            
            # Проверка низкого заряда батареи
            if parsed_level < 20:  # Порог низкого заряда
                trigger = "BATTERY_LOW"
        except ValueError:
            logger.warning(f"Не удалось распарсить уровень батареи: {level_str}")
    elif message.startswith("WDH="):
        try:
            updates["wdh_hours"] = int(message.split("=", 1)[1])
        except ValueError:
            logger.warning(f"Не удалось распарсить WDH: {message}")
    elif message.startswith("WLH="):
        try:
            updates["wlh_hours"] = int(message.split("=", 1)[1])
        except ValueError:
            logger.warning(f"Не удалось распарсить WLH: {message}")
    elif message.startswith("F_CODE="):
        updates["error_code"] = message
        trigger = "ERROR"
        updates["current_command"] = None  # Clear command on error
        SHUTTLE_ERRORS_TOTAL.labels(shuttle_id=shuttle_id, f_code=message).inc()

    # Применяем конечный автомат для определения нового состояния
    if trigger and current_state:
        new_state = await shuttle_state_machine.try_transition(
            shuttle_id, 
            current_state.status, 
            trigger, 
            {"message": message, "externaIID": current_externaIID}
        )
        if new_state:
            updates["status"] = new_state
            logger.info(f"Шаттл {shuttle_id}: переход в состояние {new_state} по триггеру {trigger}")

    # Send webhook with the externaIID retrieved from the state
    if conf.WMS_WEBHOOK_URL and conf.WMS_WEBHOOK_URL.strip():
        asyncio.create_task(send_to_wms_webhook(
            shuttle_id,
            message,
            updates.get("status", "UNKNOWN"),  # Use the potentially updated status
            updates.get("error_code"),  # Use the potentially updated error code
            current_externaIID  # Pass the externaIID from the state
        ))

    await update_shuttle_state_crud(shuttle_id, updates)
    logger.debug(f"Обработано сообщение от {shuttle_id}: {message}, обновления: {updates}")


async def handle_shuttle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    ACTIVE_SHUTTLE_CONNECTIONS.inc()
    peername = writer.get_extra_info('peername')
    shuttle_ip, shuttle_port = peername
    logger.info(f"Шаттл подключился: {shuttle_ip}:{shuttle_port}")

    shuttle_id = None
    for sid, conf in settings.SHUTTLES_CONFIG.items():
        if conf.host == shuttle_ip:
            shuttle_id = sid
            break

    if not shuttle_id:
        logger.warning(f"Неизвестный шаттл с {shuttle_ip}. Закрываем соединение.")
        writer.close()
        await writer.wait_closed()
        ACTIVE_SHUTTLE_CONNECTIONS.dec()
        return

    try:
        last_seen = time.time()
        while True:
            try:
                data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=settings.TCP_READ_TIMEOUT)
                message_str = data.decode('utf-8').strip()
                logger.info(f"Получено сообщение от {shuttle_id} ({shuttle_ip}): '{message_str}'")

                SHUTTLE_MESSAGES_RECEIVED_TOTAL.labels(shuttle_id=shuttle_id, message_type=message_str.split("=")[
                    0] if "=" in message_str else message_str).inc()
                try:
                    await process_shuttle_message_internal(shuttle_id, message_str)
                except Exception as e:
                    if settings.DEBUG:
                        logger.error(f"Ошибка при обработке сообщения от шаттла {shuttle_id}: {e}", exc_info=True)
                    else:
                        logger.error(f"Ошибка при обработке сообщения от шаттла {shuttle_id}: {e}")
                last_seen = time.time()

                if message_str.upper() != ShuttleCommand.MRCD.value:
                    writer.write(f"{ShuttleCommand.MRCD.value}\n".encode('utf-8'))
                    await asyncio.wait_for(writer.drain(), timeout=settings.TCP_WRITE_TIMEOUT)
                    logger.info(f"Отправлен MRCD шаттлу {shuttle_id} для '{message_str}'")
            except asyncio.TimeoutError:
                current_time = time.time()
                if current_time - last_seen > conf.SHUTTLE_TIMEOUT_SECONDS:
                    logger.error(f"Шаттл {shuttle_id} не отвечает более {conf.SHUTTLE_TIMEOUT_SECONDS} секунд")
                    await update_shuttle_state_crud(shuttle_id, {
                        "status": ShuttleOperationalStatus.ERROR,
                        "error_code": "TIMEOUT_NO_RESPONSE",
                        "last_seen": current_time
                    })
                    break
                continue
            except asyncio.IncompleteReadError:
                logger.warning(f"Шаттл {shuttle_id} закрыл соединение (IncompleteReadError)")
                break
    except Exception as e:
        logger.error(f"Ошибка обработки шаттла {shuttle_id}: {e}", exc_info=True)
    finally:
        logger.info(f"Закрытие соединения с {shuttle_id} ({shuttle_ip})")
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        ACTIVE_SHUTTLE_CONNECTIONS.dec()


async def start_shuttle_listener_server():
    server = await asyncio.start_server(
        handle_shuttle_client,
        '0.0.0.0',
        settings.SHUTTLE_LISTENER_PORT
    )
    addr = server.sockets[0].getsockname()
    logger.info(f"Шлюз слушает шаттлы на {addr[0]}:{addr[1]}")
    async with server:
        await server.serve_forever()
