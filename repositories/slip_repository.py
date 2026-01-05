from .repo_base import BaseRepository

class SlipHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_SLIP"""
    _table_name = "dbo.HOLD_SLIP"
    _columns = ["StoreNo","OutToStore","StatusCode","DocNumber","DocReference","Notes","Comment1","Comment2","Comment3","SlipDate","ChangeDate","LineCount","Process1","Process2","Process3"]
    #_columns = ["StoreNo","OutToStore","StatusCode","DocNumber","TrackNo","SealNo","Freight","Notes","Comment1","Comment2","Comment3","Carrier","SlipDate","SlipLines","Process1","Process2","Process3"]