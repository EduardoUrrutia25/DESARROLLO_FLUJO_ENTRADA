from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

from .base_dto import BaseHoldDTO

@dataclass
class PromoCouponHoldDTO(BaseHoldDTO):
    """DTO para insertar un cupón asociado a una promoción."""
    PromoId         : int
    Comment         : str
    CouponCode      : str
    Amount          : Optional[Decimal]
    StatusCode      : str