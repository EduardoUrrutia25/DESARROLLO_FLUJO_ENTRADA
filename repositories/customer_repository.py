from .repo_base import BaseRepository

class CustomerHoldRepository(BaseRepository):
    """Repositorio específico para la tabla HOLD_CUSTOMER_NEW"""
    _table_name = "dbo.HOLD_CUSTOMER_NEW"
    _columns = ["CustomerNo","CustomerType","UDF5","UDF6","LicenseNumber","Info1","StatusCode","FirstName","LastName","MaidenName","CompanyName","Gender","Email","CreationDate","CreatedBy","Process1","Process2","Process3"]