from .repo_base import BaseRepository

class POHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_PO"""
    _table_name = "dbo.HOLD_PO"
    _columns = ["POId","StatusCode","PONumber","PODate","CancelDate","ShipDate","StoreNo","Comment1","Comment2","Comment3","Notes","CreatedBy","CreationDate","ModifiedBy","ChangeDate","PONo","TrackNo","DocReference"]