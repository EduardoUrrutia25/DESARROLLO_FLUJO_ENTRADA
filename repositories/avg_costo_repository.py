from .repo_base import BaseRepository

class AVGCostHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_AVGCOST"""
    _table_name = "dbo.HOLD_AVGCOST"
    _columns = ["SKU","LastCost","AvgCost","CreationDate","CreatedBy","Process1","Process2","Process3"]