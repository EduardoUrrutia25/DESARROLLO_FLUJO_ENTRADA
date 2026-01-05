from .repo_base import BaseRepository

class ReceiptHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_RECEIPT_INSERT"""
    _table_name = "dbo.HOLD_RECEIPT_INSERT"
    _columns = ["StoreNo","ReceiptId","ReceiptNo","SalesCode","SalesDate","DocReference","DocNumber","Cashier","SubTotal","TaxPercent","ShippingTotal","ShipViaName","FeeTotal","FeeName","PayTotal","DiscTotal","SpecialTaxAmount","Notes","Notes2","Notes3","Flag1","Flag2","Flag3","ReferenceId","EReceipt","EReceiptInfo","BillCustomer","ShipCustomer","Process1","Process2","Process3","Type_Document","ID_Document","FullName","Address"]

    _decimal_map = {
        'SubTotal': (18, 5),
        'TaxPercent': (18, 5),
        'ShippingTotal': (18, 5),
        'FeeTotal': (18, 5),
        'PayTotal': (18, 5),
        'DiscTotal': (18, 5),
        "SpecialTaxAmount": (18, 5)
    }