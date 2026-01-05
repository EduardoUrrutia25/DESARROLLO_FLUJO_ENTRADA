from .repo_base import BaseRepository

class ProductStoreHoldRepository(BaseRepository):
    """REpositorio para la tabla HOLDER_PRODUCT_STORE"""
    _table_name = "dbo.HOLDER_PRODUCT_STORE"
    _columns = ["StoreNo","SKU","OnHandQty","OnOrderedQty","CommittedQty","InTransitQty","ProcessedFlag","ProcessedDate"]