from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal

from .base_dto      import BaseHoldDTO

@dataclass
class SlipHoldDTO(BaseHoldDTO):
    """DTO para insertar en modificación de transferencias"""
    StoreNo         :	Optional[int] 
    OutToStore      :	Optional[int] 
    StatusCode      :	str
    DocNumber       :	str
    DocReference    :	str
    Notes           :	str
    Comment1        :	str
    Comment2        :	str
    Comment3        :	str
    SlipDate        :	datetime
    ChangeDate      :	datetime
    LineCount       :	Optional[int] 
    Process1        :	Optional[int]
    Process2        :	Optional[int]
    Process3        :	Optional[int]

    #StoreNo	    : Optional[int]
    #OutToStore	: Optional[int]
    #StatusCode	: str
    #DocNumber	: str
    #TrackNo	    : str
    #SealNo	    : str
    #Freight	    : Optional[Decimal]
    #Notes	    : str
    #Comment1	: str
    #Comment2	: str
    #Comment3	: str
    #Carrier	    : str
    #SlipDate	: str
    #SlipLines	: str
    #Process1	: Optional[int]
    #Process2	: Optional[int]
    #Process3	: Optional[int]