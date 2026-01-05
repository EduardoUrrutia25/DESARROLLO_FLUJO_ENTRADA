from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal
from uuid           import UUID

from .base_dto      import BaseHoldDTO

@dataclass
class ReceiptLineHoldDTO(BaseHoldDTO):
    """DTO para insertar en Line de transaccioes de venta"""
    StoreNo	                : int
    LineId	                : int
    SKU	                    : int
    Qty	                    : Decimal
    OriginalPrice	        : Decimal
    RetailPrice	            : Decimal
    DiscPercent	            : Decimal
    LineNotes	            : Optional[str]         #NULL
    AlternativeLookUpCode	: Optional[str]         #NULL
    Clerk	                : Optional[str]         #NULL
    TaxPercent	            : Optional[Decimal]
    SalesCode	            : str
    #ReceiptId	            : Optional[UUID]
    ReceiptId	            : str