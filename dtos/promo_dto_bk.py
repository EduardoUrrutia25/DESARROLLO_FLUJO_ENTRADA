from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

from .base_dto import BaseHoldDTO

@dataclass
class PromoHoldDTO(BaseHoldDTO):
    """DTO para insertar una nueva promoción."""
    PromoId                 : Optional[int]
    Comment                 : str
    PromoName               : Optional[str]
    StartDate               : datetime
    EndDate                 : datetime
    StatusCode              : str
    ReasonCode              : Optional[str]
    TypeCode                : str
    SubTypeCode             : Optional[str]
    Qty1                    : Optional[Decimal]
    ApplyOverExistingPromo  : bool
    GetPercentOff           : Optional[Decimal]
    GetMoneyOff             : Optional[Decimal]
    ApplyOverOriginalPrice  : bool
    SpendMoney              : Optional[Decimal]
    PromoTenderId           : Optional[int]
    UseCouponsAsTender      : bool
    ItemType                : str
    SKU                     : Optional[int]
    Price                   : Optional[Decimal]
    GroupNo                 : Optional[int]
    GroupQty                : Optional[Decimal]
    Deleted                 : bool
    #Process1                : Optional[int]
    #Process2                : Optional[int]
    #Process3                : Optional[int]