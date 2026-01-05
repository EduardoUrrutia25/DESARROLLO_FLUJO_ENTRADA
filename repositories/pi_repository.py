from .repo_base import BaseRepository

class PIHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_PI"""
    _table_name = "dbo.HOLD_PI"
    _columns = ["PINumber","StoreNo","TypeCode","StatusCode","CreatedBy","CreationDate","ChangeDate","Notes","SystemQty","PhysicalQty","StartDate","StartBy","EndDate","EndBy","Process1","Process2","Process3"]