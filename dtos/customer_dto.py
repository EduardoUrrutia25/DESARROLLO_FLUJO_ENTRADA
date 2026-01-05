from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

from .base_dto import BaseHoldDTO

@dataclass
class CustomerHoldDTO(BaseHoldDTO):
    """DTO para insertar un nuevo cliente"""
    CustomerNo      : Optional[int]
    CustomerType    : str
    UDF5            : Optional[int]
    UDF6            : Optional[int]
    LicenseNumber   : str
    Info1           : Optional[str]
    StatusCode      : str
    FirstName       : str
    LastName        : Optional[str]
    MaidenName      : str
    CompanyName     : Optional[str]
    Gender          : str
    Email           : Optional[str]
    CreationDate    : datetime
    CreatedBy       : str
    Process1        : Optional[int]
    Process2        : Optional[int]
    Process3        : Optional[int]