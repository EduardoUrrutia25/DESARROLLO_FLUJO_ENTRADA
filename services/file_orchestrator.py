import os
import asyncio

from .customer_service          import CustomerFileService
from .customer_address_service  import CustomerAddressFileService
from .promo_service             import PromoFileService
from .promo_coupon_service      import PromoCouponFileService
from .promo_store_service       import PromoStoreFileService
from .avg_cost_service          import AVGCostFileService
from .product_store_service     import ProductStoreFileService
from .receipt_service           import ReceiptFileService
from .receipt_line_service      import ReceiptLineFileService
from .receipt_tender_service    import ReceiptTenderFileService

from .pi_service                import PIFileService
from .pi_line_service           import PILineFileService
from .slip_service              import SlipFileService
from .po_service                import POFileService


class FileOrchestrator:
    """
    Orquesta el procesamiento de archivos, seleccionando el servicio adecuado según el tipo de archivo.
    """
    def __init__(self):
        # Asocia un identificador en el nombre del archivo con la clase de servicio que debe procesarlo.
        self.service_map = {
            #'customer'                      : CustomerFileService,              #1.6    ###   fase 1
            #'customeraddress'               : CustomerAddressFileService,       #1.6    ###   fase 1
            'PROMO'                         : PromoFileService,                 #1.4    ###   fase 1
            'PROMOCOUPON'                   : PromoCouponFileService,           #1.4    ###   fase 1
            'PROMOSTORE'                    : PromoStoreFileService,            #1.4    ###   fase 1
            #'avgcost'                       : AVGCostFileService,               #1.9    ###   fase 1
            #'productstore'                  : ProductStoreFileService,          #1.10   ###   fase 1
            'SALESTRANSACTIONS'             : ReceiptFileService,               #1.12   ###   fase 1
            'SALESTRANSACTIONSLINE'         : ReceiptLineFileService,           #1.12   ###   fase 1
            'SALESTRANSACTIONSTENDER'       : ReceiptTenderFileService      #1.12   ###   fase 1
            #'Inventorycreation'             : PIFileService,                    #1.3    ###   fase 2
            #'InventorycreationLine'         : PILineFileService,                #1.3    ###   fase 2
            #'TransferModification'          : SlipFileService,                  #1.2    ###   fase 2
            #'PurchaseOrderModification'     : POFileService                     #1.1    ###   fase 2
        }
        print("Orquestador listo para procesar archivos.")

    #def process_file(self, file_path: str):
    async def process_file(self, file_path: str, flow_type: str):
        
        if flow_type in self.service_map:
            print(f"Archivo '{os.path.basename(file_path)}' asignado al servicio: '{flow_type}'.")
            
            service_class = self.service_map[flow_type]
            service_instance = service_class() # Instancia sin parámetros
            #service_instance.process(file_path) # El servicio se encarga de todo
            await service_instance.process(file_path)
        else:
            raise ValueError(f"No se encontró un servicio para el flujo '{flow_type}'.")