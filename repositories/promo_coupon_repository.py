from .repo_base import BaseRepository

class PromoCouponHoldRepository(BaseRepository):
    """Repositorio para la tabla HOLD_PROMO_COUPON"""
    _table_name = "dbo.HOLD_PROMO_COUPON"
    _columns = ["PromoId","Comment","CouponCode","Amount","StatusCode"]

    _decimal_map = {
        'Amount': (18, 5)
    }