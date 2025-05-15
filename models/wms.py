from pydantic import BaseModel
from typing import List, Optional
from models.shuttle import ShuttleCommand


class PlacementLine(BaseModel):
    externaIID: str
    ShuttleIN: ShuttleCommand
    params: Optional[str] = None  # Добавлено для гибкости, если параметры появятся позже


class Placement(BaseModel):
    externaIID: str
    number: str
    document: str
    nameStockERP: str
    placementLine: List[PlacementLine]


class WMSCommandPayload(BaseModel):
    placement: List[Placement]