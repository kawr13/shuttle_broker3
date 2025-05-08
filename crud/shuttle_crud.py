import json

import time # Уже импортировано
from typing import Dict, Optional, List # Уже импортировано
from models.shuttle import ShuttleState, ShuttleOperationalStatus # Уже импортировано
from core.config import settings # Уже импортировано
from core.logging_config import logger # Уже импортировано
from core.redis_client import get_redis_client # Уже импортировано
import redis.asyncio as redis # Уже импортировано

SHUTTLE_STATE_PREFIX = "shuttle_state:"


async def _get_shuttle_key(shuttle_id: str) -> str:
    return f"{SHUTTLE_STATE_PREFIX}{shuttle_id}"


async def init_shuttle_states_redis():
    r_crud = get_redis_client()  # Используем локальную переменную, чтобы не путать с импортом
    for shuttle_id in settings.SHUTTLES_CONFIG.keys():
        key = await _get_shuttle_key(shuttle_id)
        if not await r_crud.exists(key):
            initial_state = ShuttleState(shuttle_id=shuttle_id)
            await r_crud.set(key, initial_state.model_dump_json())
            logger.info(f"Инициализировано состояние для шаттла {shuttle_id} в Redis.")


async def get_shuttle_state_crud(shuttle_id: str) -> Optional[ShuttleState]:  # Переименовал во избежание конфликта
    r_crud = get_redis_client()
    key = await _get_shuttle_key(shuttle_id)
    state_json = await r_crud.get(key)
    if state_json:
        return ShuttleState.model_validate_json(state_json)
    return None


async def update_shuttle_state_crud(shuttle_id: str, updates: Dict) -> Optional[ShuttleState]:  # Переименовал
    r_crud = get_redis_client()
    key = await _get_shuttle_key(shuttle_id)

    # Оптимистическая блокировка с использованием WATCH
    async with r_crud.pipeline(transaction=True) as pipe:
        while True:  # Цикл для повторной попытки в случае WatchError
            try:
                await pipe.watch(key)
                current_state_json = await pipe.get(key)

                if not current_state_json:
                    logger.warning(f"Попытка обновить состояние несуществующего шаттла {shuttle_id} в Redis.")
                    # Можно создать, если нужно:
                    # current_state_obj = ShuttleState(shuttle_id=shuttle_id, **updates)
                    # current_state_obj.last_seen = time.time()
                    # pipe.multi() # Начать транзакцию
                    # await pipe.set(key, current_state_obj.model_dump_json())
                    # await pipe.execute()
                    # return current_state_obj
                    return None

                current_state_obj = ShuttleState.model_validate_json(current_state_json)

                updated_data = current_state_obj.model_dump()
                updated_data.update(updates)
                updated_data["last_seen"] = time.time()

                # writer не хранится в Redis
                updated_data.pop("writer", None)

                new_state_obj = ShuttleState(**updated_data)
                pipe.multi()  # Начать транзакцию перед командами изменения
                await pipe.set(key, new_state_obj.model_dump_json())
                await pipe.execute()  # Попытка выполнить транзакцию

                logger.debug(f"Состояние шаттла {shuttle_id} обновлено в Redis: {new_state_obj}")
                return new_state_obj  # Успех, выходим из цикла
            except redis.WatchError:
                logger.warning(
                    f"Конфликт записи (WatchError) при обновлении состояния шаттла {shuttle_id}. Повторная попытка.")
                # Ключ был изменен другой транзакцией, продолжаем цикл для повторной попытки
                continue  # Повторяем операцию WATCH и GET
            except Exception as e:
                logger.error(
                    f"Ошибка при обновлении состояния шаттла {shuttle_id} в Redis: {e}. Данные: {updated_data if 'updated_data' in locals() else 'N/A'}")
                return current_state_obj if 'current_state_obj' in locals() else None  # Возвращаем старое состояние или None


async def get_all_shuttle_states_crud() -> Dict[str, ShuttleState]:  # Переименовал
    r_crud = get_redis_client()
    keys = []
    async for key_bytes in r_crud.scan_iter(match=f"{SHUTTLE_STATE_PREFIX}*"):
        keys.append(key_bytes)  # scan_iter возвращает байты

    states = {}
    if keys:
        for key_str in keys:
            state_json = await r_crud.get(key_str)
            if state_json:
                try:
                    shuttle_id = key_str.replace(SHUTTLE_STATE_PREFIX, "")
                    states[shuttle_id] = ShuttleState.model_validate_json(state_json)
                except Exception as e:
                    logger.error(f"Ошибка десериализации состояния для ключа {key_str}: {e}")
    return states