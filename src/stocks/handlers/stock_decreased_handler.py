"""
Handler: Stock Decreased
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import requests
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer


class StockDecreasedHandler(EventHandler):
    """Handles StockDecreased events"""

    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()

    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreased"

    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        # Le stock a été diminué, alors créez la transaction de paiement (appel synchrone à l'API Payments, comme au labo 6).
        # NOTE: l'activité 6 (Outbox) remplacera cet appel direct par un enregistrement dans la table Outbox + OutboxProcessor.
        try:
            response = requests.post('http://api-gateway:8080/payments-api/payments',
                json={
                    "user_id": event_data['user_id'],
                    "order_id": event_data['order_id'],
                    "total_amount": event_data['total_amount']
                },
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            if not response.ok:
                raise Exception(f"Erreur {response.status_code} : {response.text}")

            # Si la transaction de paiement a été crée, déclenchez PaymentCreated.
            event_data['payment_id'] = response.json()['payment_id']
            event_data['event'] = "PaymentCreated"
        except Exception as e:
            # Si la transaction de paiement n'était pas crée, déclenchez PaymentCreationFailed.
            self.logger.debug("La création d'une transaction de paiement a échoué : " + str(e))
            event_data['event'] = "PaymentCreationFailed"
            event_data['error'] = str(e)
        finally:
            self.logger.debug(f"payment_link={event_data['payment_link']}")
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
