from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal

from .base_dto      import BaseHoldDTO

@dataclass
class AVGCostHoldDTO(BaseHoldDTO):
    """DTO para insertar en costo promedio"""
    SKU             : Optional[int]
    LastCost        : Optional[Decimal]
    AvgCost         : Optional[Decimal]
    CreationDate    : datetime
    CreatedBy       : str
    Process1        : Optional[int]
    Process2        : Optional[int]
    Process3        : Optional[int]