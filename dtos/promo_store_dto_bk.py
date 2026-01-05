from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

from .base_dto import BaseHoldDTO

@dataclass
class PromoStoreHoldDTO(BaseHoldDTO):
    """DTO para insertar una tienda asociado a una promoción."""
    PromoId         : Optional[int]
    Comment         : str
    StoreNo         : Optional[int]
    Deleted         : Optional[int]