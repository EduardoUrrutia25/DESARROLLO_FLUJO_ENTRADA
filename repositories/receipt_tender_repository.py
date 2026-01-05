from .repo_base import BaseRepository

class ReceiptTenderHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLD_RECEIPT_TENDER_INSERT"""
    _table_name = "dbo.HOLD_RECEIPT_TENDER_INSERT"
    _columns = ["StoreNo","LineId","ReceiptId","TenderId","PaymentDate","CardName","CardNumber","CardExpDate","CardAuthorization","CardZipCode","CardSequenceNumber","EFT","CheckNumber","GiftCertNumber","TakeAmount","GiveAmount","Notes","DebitSale","PaymentDay","CurrencyId","ExchangeRate","TakeBase","TakeExchange"]

    _decimal_map = {
        'TakeAmount': (18, 5),
        'GiveAmount': (18, 5),
        'ExchangeRate': (18, 5),
        'TakeBase': (18, 5),
        'TakeExchange': (18, 5)
    }