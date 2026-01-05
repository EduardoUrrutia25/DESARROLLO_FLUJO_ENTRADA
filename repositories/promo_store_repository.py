from .repo_base import BaseRepository

class PromoStoreHoldRepository(BaseRepository):
    """Repositorio para la tabla HOLD_PROMO_STORE"""
    _table_name = "dbo.HOLD_PROMO_STORE"
    _columns = ["PromoId","Comment","StoreNo","Deleted"]