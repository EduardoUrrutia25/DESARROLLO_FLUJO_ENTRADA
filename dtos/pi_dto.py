from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal

from .base_dto      import BaseHoldDTO

@dataclass
class PIHoldDTO(BaseHoldDTO):
    """DTO para insertar en creación de inventarios"""
    PINumber	    : str
    StoreNo	        : Optional[int]
    TypeCode	    : str
    StatusCode	    : str
    CreatedBy	    : str
    CreationDate	: datetime
    ChangeDate	    : datetime
    Notes	        : str
    SystemQty	    : Optional[Decimal]
    PhysicalQty	    : Optional[Decimal]
    StartDate	    : datetime
    StartBy	        : str
    EndDate	        : datetime
    EndBy	        : str
    Process1	    : Optional[int]
    Process2	    : Optional[int]
    Process3	    : Optional[int]