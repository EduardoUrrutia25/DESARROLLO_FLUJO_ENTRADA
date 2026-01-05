from .repo_base import BaseRepository

class PromoHoldRepository(BaseRepository):
    """Repositorio para la tabla HOLD_PROMO"""
    _table_name = "dbo.HOLD_PROMO"
    _columns = ["PromoId","Comment","PromoName","StartDate","EndDate","StatusCode","ReasonCode","TypeCode","SubTypeCode","Qty1","ApplyOverExistingPromo","GetPercentOff","GetMoneyOff","ApplyOverOriginalPrice","SpendMoney","PromoTenderId","UseCouponsAsTender","ItemType","SKU","Price","GroupNo","GroupQty","Deleted"]    

    _decimal_map = {
        'Qty1': (18, 5),
        'GetPercentOff': (18, 5),
        'GetMoneyOff': (18, 5),
        'SpendMoney': (18, 5),
        'Price': (18, 5),
        'GroupQty': (18, 5)
    }