from .repo_base import BaseRepository

class PILineHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_PI_LINE"""
    _table_name = "dbo.HOLD_PI_LINE"
    _columns = ["PINumber","StoreNo","SKU","SystemQty","PhysicalQty","DiffQty","AvgCost"]