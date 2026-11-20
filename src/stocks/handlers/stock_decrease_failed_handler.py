"""
Handler: Stock Decrease Failed
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from orders.commands.write_order import delete_order


class StockDecreaseFailedHandler(EventHandler):
    """Handles StockDecreaseFailed events"""

    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()

    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreaseFailed"

    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        # Le stock n'a pas pu être diminué : compensation = annuler (supprimer) la commande.
        try:
            delete_order(event_data['order_id'])
        except Exception as e:
            # Il n'y a pas d'étape précédente à compenser : on note l'erreur et on termine la saga quand même
            # (pas de rollback sur les rollbacks, comme au labo 6).
            self.logger.error(f"La suppression de la commande {event_data.get('order_id')} a échoué : {e}")
            event_data['error'] = str(e)
        finally:
            event_data['event'] = "OrderCancelled"
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
