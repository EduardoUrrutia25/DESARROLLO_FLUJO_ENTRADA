from .customer_service          import CustomerFileService
from .customer_address_service  import CustomerAddressFileService
from .promo_service             import PromoFileService
from .promo_coupon_service      import PromoCouponFileService
from .promo_store_service       import PromoStoreFileService
from .avg_cost_service          import AVGCostFileService
from .product_store_service     import ProductStoreFileService
from .receipt_service           import ReceiptFileService
from .receipt_line_service      import ReceiptLineFileService
from .receipt_tender_service    import ReceiptTenderHoldRepository

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
            'customer'                      : CustomerFileService,              #1.6    ###   fase 1
            'customeraddress'               : CustomerAddressFileService,       #1.6    ###   fase 1
            'promo'                         : PromoFileService,                 #1.4    ###   fase 1
            'promocoupon'                   : PromoCouponFileService,           #1.4    ###   fase 1
            'promostore'                    : PromoStoreFileService,            #1.4    ###   fase 1
            'avgcost'                       : AVGCostFileService,               #1.9    ###   fase 1
            'productstore'                  : ProductStoreFileService,          #1.10   ###   fase 1
            'SalesTransactions'             : ReceiptFileService,               #1.12   ###   fase 1
            'SalesTransactionsLine'         : ReceiptLineFileService,           #1.12   ###   fase 1
            'SalesTransactionsTender'       : ReceiptTenderHoldRepository,      #1.12   ###   fase 1
            'Inventorycreation'             : PIFileService,                    #1.3    ###   fase 2
            'InventorycreationLine'         : PILineFileService,                #1.3    ###   fase 2
            'TransferModification'          : SlipFileService,                  #1.2    ###   fase 2
            'PurchaseOrderModification'     : POFileService                     #1.1    ###   fase 2
        }
        print("Orquestador listo para procesar archivos.")

    #def process_file(self, file_path: str):
    def process_file(self, file_path: str, db_identifier: str):
        try:
            #file_name = file_path.split('/')[-1].lower()
            file_name = file_path.split('/')[-1]
            
            best_match_key = None
            for key in self.service_map.keys():
                if key in file_name:
                    # Si encontramos una clave que es más larga (más específica)
                    # que la que ya teníamos, la reemplazamos.
                    if best_match_key is None or len(key) > len(best_match_key):
                        best_match_key = key

            if best_match_key:
                print(f"\nArchivo '{file_name}' detectado. Tipo de flujo: '{best_match_key}'.")
                service_class = self.service_map[best_match_key]
                #service_instance = service_class()
                #service_instance.process(file_path)
                service_instance = service_class(db_identifier=db_identifier)
                service_instance.process(file_path)
            else:
                print(f"ADVERTENCIA: No se encontró un servicio para procesar el archivo '{file_name}'.")

        except Exception as e:
            print(f"ERROR: Ocurrió un error fatal al procesar '{file_path}': {e}")