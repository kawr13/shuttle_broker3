import asyncio
import time
from typing import Dict, Set

from core.config import settings
from core.logging_config import logger
from crud.shuttle_crud import get_shuttle_state_crud, update_shuttle_state_crud
from models.shuttle import ShuttleOperationalStatus
from services.shuttle_comms import send_command_to_shuttle
from services.retry_mechanism import retry_with_backoff

class HeartbeatMonitor:
    def __init__(self):
        self.heartbeat_interval = 30  # секунды между проверками
        self.shuttle_timeouts: Dict[str, float] = {}
        self.failed_shuttles: Set[str] = set()
        self.running = False
        self.task = None
    
    async def start(self):
        """Запускает мониторинг шаттлов"""
        if self.running:
            return
        
        self.running = True
        self.task = asyncio.create_task(self._monitor_loop())
        logger.info("Запущен мониторинг состояния шаттлов")
    
    async def stop(self):
        """Останавливает мониторинг шаттлов"""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("Мониторинг состояния шаттлов остановлен")
    
    async def _monitor_loop(self):
        """Основной цикл мониторинга шаттлов"""
        while self.running:
            try:
                await self._check_all_shuttles()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}", exc_info=True)
                await asyncio.sleep(5)  # Короткая пауза перед повторной попыткой
    
    async def _check_all_shuttles(self):
        """Проверяет состояние всех шаттлов"""
        current_time = time.time()
        
        for shuttle_id in settings.SHUTTLES_CONFIG.keys():
            try:
                state = await get_shuttle_state_crud(shuttle_id)
                if not state:
                    continue
                
                # Проверяем, когда последний раз видели шаттл
                time_since_last_seen = current_time - state.last_seen
                
                # Если шаттл не отвечал долго или находится в состоянии ошибки
                if (time_since_last_seen > self.heartbeat_interval * 2 or 
                    state.status == ShuttleOperationalStatus.ERROR):
                    
                    if shuttle_id not in self.failed_shuttles:
                        logger.warning(f"Шаттл {shuttle_id} не отвечает или в состоянии ошибки. "
                                      f"Последняя активность: {time_since_last_seen:.1f} сек назад")
                        self.failed_shuttles.add(shuttle_id)
                        
                    # Пытаемся восстановить соединение
                    await self._try_reconnect(shuttle_id)
                elif shuttle_id in self.failed_shuttles:
                    # Шаттл снова активен
                    logger.info(f"Шаттл {shuttle_id} снова активен")
                    self.failed_shuttles.remove(shuttle_id)
            except Exception as e:
                logger.error(f"Ошибка при проверке шаттла {shuttle_id}: {e}")
    
    async def _try_reconnect(self, shuttle_id: str):
        """Пытается восстановить соединение с шаттлом"""
        try:
            # Отправляем STATUS команду для проверки соединения
            success = await retry_with_backoff(
                send_command_to_shuttle,
                shuttle_id, 
                "STATUS",
                max_retries=2,
                base_delay=2.0
            )
            
            if success:
                logger.info(f"Соединение с шаттлом {shuttle_id} восстановлено")
                await update_shuttle_state_crud(shuttle_id, {
                    "status": ShuttleOperationalStatus.UNKNOWN,  # Сбрасываем статус до получения ответа
                    "error_code": None
                })
                self.failed_shuttles.discard(shuttle_id)
            else:
                logger.warning(f"Не удалось восстановить соединение с шаттлом {shuttle_id}")
        except Exception as e:
            logger.error(f"Ошибка при попытке восстановления соединения с шаттлом {shuttle_id}: {e}")

# Создаем глобальный экземпляр монитора
heartbeat_monitor = HeartbeatMonitor()