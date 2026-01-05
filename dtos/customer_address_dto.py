from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

from .base_dto import BaseHoldDTO

@dataclass
class CustomerAddressHoldDTO(BaseHoldDTO):
    """DTO para insertar una dirección de cliente"""
    CustomerNo  : Optional[int]
    Address1    : Optional[str]
    City        : Optional[str]
    State       : Optional[str]
    Abbrev      : str