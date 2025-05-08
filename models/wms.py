from pydantic import BaseModel # Уже импортировано
from typing import Optional, Dict, Any # Уже импортировано
from models.shuttle import ShuttleCommand # Уже импортировано

class WMSCommandPayload(BaseModel):
    shuttle_id: str
    command: ShuttleCommand
    params: Optional[str] = None
    # api_payload: Optional[Dict[str, Any]] = None # Если нужно