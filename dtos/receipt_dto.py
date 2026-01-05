from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal
from uuid           import UUID

from .base_dto      import BaseHoldDTO

@dataclass
class ReceiptHoldDTO(BaseHoldDTO):
    """DTO para insertar en transaccioes de venta"""
    StoreNo	            : int
    #ReceiptId	        : Optional[UUID]
    ReceiptId	        : str
    ReceiptNo	        : Optional[int]
    SalesCode	        : str
    SalesDate	        : datetime
    DocReference	    : Optional[str]
    DocNumber	        : str
    Cashier	            : str
    SubTotal	        : Decimal
    TaxPercent	        : Decimal
    ShippingTotal	    : Decimal
    ShipViaName	        : Optional[str]
    FeeTotal	        : Optional[Decimal]     #NULL
    FeeName	            : Optional[str]         #NULL
    PayTotal	        : Decimal
    DiscTotal	        : Decimal
    SpecialTaxAmount	: Optional[Decimal]     #0
    Notes	            : Optional[str]         #NULL
    Notes2	            : Optional[str]         #NULL
    Notes3	            : Optional[str]         #NULL
    Flag1	            : int
    Flag2	            : Optional[int]         #NULL
    Flag3	            : Optional[int]         #NULL
    ReferenceId	        : Optional[str]         #NULL
    EReceipt	        : Optional[str]         #NULL
    EReceiptInfo	    : Optional[str]         #NULL
    BillCustomer	    : str
    ShipCustomer	    : str
    Process1            : bool                  #0
    Process2            : bool                  #0
    Process3            : bool                  #0
    Type_Document       : int
    ID_Document         : str
    FullName            : str
    Address             : str
