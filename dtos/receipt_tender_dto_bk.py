from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal
from uuid           import UUID

from .base_dto      import BaseHoldDTO

@dataclass
class ReceiptTenderHoldDTO(BaseHoldDTO):
    """DTO para insertar el Tender de transaccioes de venta"""
    StoreNo	            : Optional[int]
    LineId	            : Optional[int]
    ReceiptId	        : str
    TenderId	        : str
    PaymentDate	        : datetime
    CardName	        : Optional[str]         #NULL
    CardNumber	        : Optional[str]         #NULL
    CardExpDate	        : Optional[str]         #NULL
    CardAuthorization	: Optional[str]         #NULL
    CardZipCode	        : Optional[str]         #NULL
    CardSequenceNumber	: Optional[str]         #NULL
    EFT	                : int                   #0
    CheckNumber	        : Optional[str]         #NULL
    GiftCertNumber	    : Optional[str]         #NULL
    TakeAmount	        : Optional[Decimal]
    GiveAmount	        : Optional[Decimal]
    Notes	            : Optional[str]         #NULL
    DebitSale	        : Optional[str]         #NULL
    PaymentDay	        : Optional[int]         #NULL
    CurrencyId	        : Optional[int]         #NULL
    ExchangeRate	    : Optional[Decimal]     #NULL
    TakeBase	        : Optional[Decimal]     #NULL
    TakeExchange	    : Optional[Decimal]     #NULL