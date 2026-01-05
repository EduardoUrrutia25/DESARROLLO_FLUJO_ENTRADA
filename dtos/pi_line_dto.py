from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal

from .base_dto      import BaseHoldDTO

@dataclass
class PILineHoldDTO(BaseHoldDTO):
    """DTO para insertar line en creación de inventarios"""
    PINumber    : Optional[str]
    StoreNo     : Optional[int]
    SKU         : Optional[int]
    SystemQty   : Optional[Decimal]
    PhysicalQty : Optional[int]
    DiffQty     : Optional[Decimal]
    AvgCost     : Optional[Decimal]