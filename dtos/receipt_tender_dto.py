from dataclasses    import dataclass
from typing         import Optional
from datetime       import datetime
from decimal        import Decimal

from .base_dto      import BaseHoldDTO

@dataclass
class ReceiptTenderHoldDTO(BaseHoldDTO):
    """DTO para insertar el Tender de transacciones de venta"""
    
    # ===== OBLIGATORIOS (Campo Necesario = SI) - 7 campos =====
    StoreNo             : int                   # int - SI
    LineId              : int                   # int - SI
    ReceiptId           : str                   # Varchar(100) - SI
    TenderId            : str                   # Varchar(50) - SI
    PaymentDate         : datetime              # datetime - SI
    TakeAmount          : Decimal               # Decimal(18,5) - SI
    GiveAmount          : Decimal               # Decimal(18,5) - SI
    
    # ===== OPCIONALES (Campo Necesario = NO) - 16 campos =====
    CardName            : Optional[str]         # Varchar(50) - NO
    CardNumber          : Optional[str]         # Varchar(50) - NO
    CardExpDate         : Optional[str]         # Varchar(6) - NO
    CardAuthorization   : Optional[str]         # Varchar(20) - NO
    CardZipCode         : Optional[str]         # Varchar(20) - NO
    CardSequenceNumber  : Optional[str]         # Varchar(20) - NO
    EFT                 : Optional[bool]        # bit Default=0 - NO
    CheckNumber         : Optional[str]         # Varchar(50) - NO
    GiftCertNumber      : Optional[str]         # Varchar(50) - NO
    Notes               : Optional[str]         # [varchar](2000) - NO
    DebitSale           : Optional[bool]        # bit Default=NULL - NO
    PaymentDay          : Optional[int]         # int - NO
    CurrencyId          : Optional[int]         # tinyint - NO
    ExchangeRate        : Optional[Decimal]     # Decimal(18,5) - NO
    TakeBase            : Optional[Decimal]     # Decimal(18,5) - NO
    TakeExchange        : Optional[Decimal]     # Decimal(18,5) - NO