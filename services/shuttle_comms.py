import asyncio
import time
from typing import Optional

import aiohttp

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import update_shuttle_state_crud
from models.shuttle import ShuttleOperationalStatus, ShuttleCommand
from services.services import ACTIVE_SHUTTLE_CONNECTIONS, SHUTTLE_BATTERY_LEVEL, SHUTTLE_MESSAGES_RECEIVED_TOTAL, \
    SHUTTLE_ERRORS_TOTAL

SHUTTLE_TIMEOUT_SECONDS = 30  # Таймаут для зависших шаттлов
WMS_WEBHOOK_URL = settings.WMS_WEBHOOK_URL

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

    logger.info(f"Отправка команды '{full_command.strip()}' шаттлу {shuttle_id} ({shuttle_config.host}:{shuttle_config.command_port})")

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
        await update_shuttle_state_crud(shuttle_id, {"status": ShuttleOperationalStatus.ERROR, "error_code": "TCP_TIMEOUT_SEND"})
    except ConnectionRefusedError:
        logger.error(f"Соединение отклонено шаттлом {shuttle_id} ({shuttle_config.host}:{shuttle_config.command_port})")
        await update_shuttle_state_crud(shuttle_id, {"status": ShuttleOperationalStatus.ERROR, "error_code": "CONNECTION_REFUSED"})
    except OSError as e:
        logger.error(f"Ошибка сети при отправке команды шаттлу {shuttle_id}: {e}")
        await update_shuttle_state_crud(shuttle_id, {"status": ShuttleOperationalStatus.ERROR, "error_code": f"NET_ERROR_{e.errno}"})
    except Exception as e:
        logger.error(f"Неизвестная ошибка при отправке команды шаттлу {shuttle_id}: {e}")
        await update_shuttle_state_crud(shuttle_id, {"status": ShuttleOperationalStatus.ERROR, "error_code": "UNKNOWN_SEND_ERROR"})
    return False


async def send_to_wms_webhook(shuttle_id: str, message: str, status: str, error_code: Optional[str] = None):
    if not WMS_WEBHOOK_URL or not WMS_WEBHOOK_URL.strip():
        logger.warning("WMS_WEBHOOK_URL не настроен или пуст. Пропуск отправки в WMS.")
        return

    payload = {
        "shuttle_id": shuttle_id,
        "message": message,
        "status": status,
        "error_code": error_code,
        "timestamp": time.time()
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(WMS_WEBHOOK_URL, json=payload) as response:
                if response.status == 200:
                    logger.info(f"Успешно отправлено в WMS для {shuttle_id}: {message}")
                else:
                    logger.error(f"Ошибка отправки в WMS: {response.status} {await response.text()}")
    except Exception as e:
        logger.error(f"Ошибка при отправке Webhook в WMS: {e}, данные: {payload}")


async def process_shuttle_message_internal(shuttle_id: str, message: str):
    updates = {"last_message_sent_to_wms": message, "last_seen": time.time()}

    if message.endswith("_STARTED"):
        updates["status"] = ShuttleOperationalStatus.BUSY
    elif message.endswith("_DONE") or message.endswith("_ABORT"):
        updates["status"] = ShuttleOperationalStatus.FREE
        updates["current_command"] = None
        if message.endswith("_ABORT"):
            updates["error_code"] = message
            updates["status"] = ShuttleOperationalStatus.ERROR

    if message.startswith("LOCATION="):
        updates["location_data"] = message.split("=", 1)[1]
        updates["status"] = ShuttleOperationalStatus.FREE
        updates["current_command"] = None
    elif message.startswith("COUNT_") and "=" in message:
        updates["pallet_count_data"] = message
        updates["status"] = ShuttleOperationalStatus.FREE
        updates["current_command"] = None
    elif message.startswith("STATUS="):
        status_val = message.split("=", 1)[1]
        status_map = {
            "FREE": ShuttleOperationalStatus.FREE,
            "CARGO": ShuttleOperationalStatus.BUSY,
            "BUSY": ShuttleOperationalStatus.BUSY,
            "NOT_READY": ShuttleOperationalStatus.NOT_READY
        }
        updates["status"] = status_map.get(status_val, ShuttleOperationalStatus.UNKNOWN)
    elif message.startswith("BATTERY="):
        level_str = message.split("=", 1)[1]
        updates["battery_level"] = level_str
        try:
            parsed_level = float(level_str.replace('%', '').lstrip('<'))
            SHUTTLE_BATTERY_LEVEL.labels(shuttle_id=shuttle_id).set(parsed_level)
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
        updates["status"] = ShuttleOperationalStatus.ERROR
        SHUTTLE_ERRORS_TOTAL.labels(shuttle_id=shuttle_id, f_code=message).inc()

    if WMS_WEBHOOK_URL and WMS_WEBHOOK_URL.strip():
        asyncio.create_task(send_to_wms_webhook(
            shuttle_id,
            message,
            updates.get("status", "UNKNOWN"),
            updates.get("error_code")
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

                SHUTTLE_MESSAGES_RECEIVED_TOTAL.labels(shuttle_id=shuttle_id, message_type=message_str.split("=")[0] if "=" in message_str else message_str).inc()
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
                if current_time - last_seen > SHUTTLE_TIMEOUT_SECONDS:
                    logger.error(f"Шаттл {shuttle_id} не отвечает более {SHUTTLE_TIMEOUT_SECONDS} секунд")
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