from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal
from uuid           import UUID

from .base_dto      import BaseHoldDTO

@dataclass
class POHoldDTO(BaseHoldDTO):
    """DTO para insertar en modificación de orden de compra"""
    POId	        : Optional[UUID]
    StatusCode	    : str
    PONumber	    : str
    PODate	        : datetime
    CancelDate	    : datetime
    ShipDate	    : datetime
    StoreNo	        : Optional[int]
    Comment1	    : str
    Comment2	    : str
    Comment3	    : str
    Notes	        : str
    CreatedBy	    : Optional[str]
    CreationDate	: datetime
    ModifiedBy	    : str
    ChangeDate	    : datetime
    PONo	        : Optional[int]
    TrackNo	        : str
    DocReference	: str