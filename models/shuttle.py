import time  # Добавил импорт
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ShuttleCommand(str, Enum):
    PALLET_IN = "PALLET_IN"
    PALLET_OUT = "PALLET_OUT"
    FIFO_NNN = "FIFO"
    FILO_NNN = "FILO"
    STACK_IN = "STACK_IN"
    STACK_OUT = "STACK_OUT"
    HOME = "HOME"
    COUNT = "COUNT"
    STATUS = "STATUS"
    BATTERY = "BATTERY"
    WDH = "WDH"
    WLH = "WLH"
    MRCD = "MRCD"


class ShuttleMessage(str, Enum): # Не используется напрямую в коде, но полезно для понимания
    PALLET_IN_STARTED = "PALLET_IN_STARTED"
    PALLET_IN_DONE = "PALLET_IN_DONE"
    # ... (остальные сообщения, как в предыдущем примере) ...


class ShuttleOperationalStatus(str, Enum):
    FREE = "FREE"
    BUSY = "BUSY"
    ERROR = "ERROR"
    NOT_READY = "NOT_READY"
    AWAITING_MRCD = "AWAITING_MRCD"
    UNKNOWN = "UNKNOWN"


class ShuttleState(BaseModel):
    shuttle_id: str
    status: ShuttleOperationalStatus = ShuttleOperationalStatus.UNKNOWN
    current_command: Optional[str] = None
    last_message_sent_to_wms: Optional[str] = None
    last_message_received_from_wms: Optional[str] = None
    battery_level: Optional[str] = None
    location_data: Optional[str] = None
    pallet_count_data: Optional[str] = None
    wdh_hours: Optional[int] = None
    wlh_hours: Optional[int] = None
    error_code: Optional[str] = None
    last_seen: float = Field(default_factory=time.time)
    externaIID: Optional[str] = None  # Новое поле для externaIID
    # writer: Optional[asyncio.StreamWriter] = None # Убрал, т.к. не хранится в Redis и управляется локально

    class Config:
        arbitrary_types_allowed = True