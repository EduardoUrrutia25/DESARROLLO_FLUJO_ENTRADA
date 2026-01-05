from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

from .base_dto import BaseHoldDTO

@dataclass
class PromoHoldDTO(BaseHoldDTO):
    PromoId                 : int
    Comment                 : Optional[str]
    PromoName               : str
    StartDate               : datetime
    EndDate                 : datetime
    StatusCode              : str
    ReasonCode              : str
    TypeCode                : str
    SubTypeCode             : Optional[str]
    Qty1                    : Optional[Decimal]
    ApplyOverExistingPromo  : int
    GetPercentOff           : Optional[int]
    GetMoneyOff             : Optional[Decimal]
    ApplyOverOriginalPrice  : int
    SpendMoney              : Optional[Decimal]
    PromoTenderId           : Optional[int]
    UseCouponsAsTender      : Optional[int]
    ItemType                : str
    SKU                     : int
    Price                   : Optional[Decimal]
    GroupNo                 : Optional[int]
    GroupQty                : Optional[Decimal]
    Deleted                 : int
    #Process1                : Optional[int]
    #Process2                : Optional[int]
    #Process3                : Optional[int]