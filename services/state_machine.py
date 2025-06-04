from typing import Dict, Optional, Callable, Awaitable, Any

from core.logging_config import logger
from models.shuttle import ShuttleOperationalStatus, ShuttleCommand

# Определение типа для функций-обработчиков переходов
TransitionHandler = Callable[[str, str, Optional[Any]], Awaitable[None]]

class ShuttleStateMachine:
    def __init__(self):
        # Определение допустимых переходов состояний
        self.transitions = {
            # Из FREE можно перейти в эти состояния
            ShuttleOperationalStatus.FREE: {
                ShuttleCommand.PALLET_IN: ShuttleOperationalStatus.LOADING,
                ShuttleCommand.PALLET_OUT: ShuttleOperationalStatus.UNLOADING,
                ShuttleCommand.FIFO_NNN: ShuttleOperationalStatus.MOVING,
                ShuttleCommand.FILO_NNN: ShuttleOperationalStatus.MOVING,
                ShuttleCommand.STACK_IN: ShuttleOperationalStatus.LOADING,
                ShuttleCommand.STACK_OUT: ShuttleOperationalStatus.UNLOADING,
                ShuttleCommand.HOME: ShuttleOperationalStatus.MOVING,
                "BATTERY_LOW": ShuttleOperationalStatus.LOW_BATTERY,
                "ERROR": ShuttleOperationalStatus.ERROR,
            },
            # Из BUSY можно перейти в эти состояния
            ShuttleOperationalStatus.BUSY: {
                "DONE": ShuttleOperationalStatus.FREE,
                "ERROR": ShuttleOperationalStatus.ERROR,
                ShuttleCommand.HOME: ShuttleOperationalStatus.MOVING,
            },
            # Из LOADING можно перейти в эти состояния
            ShuttleOperationalStatus.LOADING: {
                "DONE": ShuttleOperationalStatus.FREE,
                "ERROR": ShuttleOperationalStatus.ERROR,
                ShuttleCommand.HOME: ShuttleOperationalStatus.MOVING,
            },
            # Из UNLOADING можно перейти в эти состояния
            ShuttleOperationalStatus.UNLOADING: {
                "DONE": ShuttleOperationalStatus.FREE,
                "ERROR": ShuttleOperationalStatus.ERROR,
                ShuttleCommand.HOME: ShuttleOperationalStatus.MOVING,
            },
            # Из MOVING можно перейти в эти состояния
            ShuttleOperationalStatus.MOVING: {
                "DONE": ShuttleOperationalStatus.FREE,
                "ERROR": ShuttleOperationalStatus.ERROR,
            },
            # Из ERROR можно перейти только в FREE после сброса ошибки
            ShuttleOperationalStatus.ERROR: {
                "RESET": ShuttleOperationalStatus.FREE,
            },
            # Из LOW_BATTERY можно перейти в CHARGING или ERROR
            ShuttleOperationalStatus.LOW_BATTERY: {
                "CHARGING": ShuttleOperationalStatus.CHARGING,
                "ERROR": ShuttleOperationalStatus.ERROR,
            },
            # Из CHARGING можно перейти в FREE когда зарядка завершена
            ShuttleOperationalStatus.CHARGING: {
                "CHARGED": ShuttleOperationalStatus.FREE,
                "ERROR": ShuttleOperationalStatus.ERROR,
            },
        }
        
        # Обработчики переходов состояний
        self.transition_handlers: Dict[str, TransitionHandler] = {}
    
    def register_transition_handler(self, transition_key: str, handler: TransitionHandler):
        """Регистрирует обработчик для конкретного перехода состояния"""
        self.transition_handlers[transition_key] = handler
    
    async def try_transition(self, shuttle_id: str, current_state: ShuttleOperationalStatus, 
                           trigger: str, context: Optional[Any] = None) -> Optional[ShuttleOperationalStatus]:
        """
        Пытается выполнить переход состояния на основе триггера.
        
        Args:
            shuttle_id: ID шаттла
            current_state: Текущее состояние шаттла
            trigger: Триггер перехода (команда или событие)
            context: Дополнительный контекст для обработчика перехода
            
        Returns:
            Новое состояние, если переход возможен, иначе None
        """
        if current_state not in self.transitions:
            logger.warning(f"Неизвестное текущее состояние {current_state} для шаттла {shuttle_id}")
            return None
            
        if trigger not in self.transitions[current_state]:
            logger.warning(f"Недопустимый переход из {current_state} по триггеру {trigger} для шаттла {shuttle_id}")
            return None
            
        new_state = self.transitions[current_state][trigger]
        logger.info(f"Переход состояния шаттла {shuttle_id}: {current_state} -> {new_state} (триггер: {trigger})")
        
        # Вызов обработчика перехода, если он зарегистрирован
        transition_key = f"{current_state}:{trigger}:{new_state}"
        if transition_key in self.transition_handlers:
            try:
                await self.transition_handlers[transition_key](shuttle_id, transition_key, context)
            except Exception as e:
                logger.error(f"Ошибка в обработчике перехода {transition_key} для шаттла {shuttle_id}: {e}")
        
        return new_state

# Создаем глобальный экземпляр конечного автомата
shuttle_state_machine = ShuttleStateMachine()