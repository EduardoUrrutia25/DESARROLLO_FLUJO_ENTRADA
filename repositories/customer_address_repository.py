from .repo_base import BaseRepository

class CustomerAddressHoldRepository(BaseRepository):
    """Repositorio específico para la tabla HOLD_CUSTOMER_ADDRESS_NEW"""
    _table_name = "dbo.HOLD_CUSTOMER_ADDRESS_NEW"
    _columns = ["CustomerNo","Address1","City","State","Abbrev"]