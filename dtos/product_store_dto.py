from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

from .base_dto import BaseHoldDTO

@dataclass
class ProductStoreHoldDTO(BaseHoldDTO):
    """DTO para insertar en product store"""
    StoreNo         : Optional[int]
    SKU             : Optional[int]
    OnHandQty       : Optional[Decimal]
    OnOrderedQty    : Optional[Decimal]
    CommittedQty    : Optional[Decimal]
    InTransitQty    : Optional[Decimal]
    ProcessedFlag   : str
    ProcessedDate   : datetime