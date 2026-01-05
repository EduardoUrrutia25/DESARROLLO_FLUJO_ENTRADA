from .repo_base import BaseRepository

class ReceiptLineHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_RECEIPT_LINE_INSERT"""
    _table_name = "dbo.HOLD_RECEIPT_LINE_INSERT"
    _columns = ["StoreNo","LineId","SKU","Qty","OriginalPrice","RetailPrice","DiscPercent","LineNotes","AlternativeLookUpCode","Clerk","TaxPercent","SalesCode","ReceiptId"]

    _decimal_map = {
        'Qty': (18, 5),
        'OriginalPrice': (18, 5),
        'RetailPrice': (18, 5),
        'DiscPercent': (18, 5),
        'TaxPercent': (18, 5)
    }