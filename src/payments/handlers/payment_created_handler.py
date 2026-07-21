"""
Handler: Payment Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from orders.commands.write_order import modify_order

class PaymentCreatedHandler(EventHandler):
    """Handles PaymentCreated events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "PaymentCreated"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        try:
            payment_id = event_data["payment_id"]

            success = modify_order(
                event_data["order_id"],
                event_data.get("is_paid", False),
                payment_id
            )

            if not success:
                raise ValueError("La commande n'a pas pu être mise à jour.")

            event_data["payment_link"] = (
                f"http://api-gateway:8080/payments-api/payments/process/{payment_id}"
            )
            event_data["event"] = "SagaCompleted"

        except Exception as e:
            event_data["event"] = "PaymentCreationFailed"
            event_data["error"] = str(e)

        OrderEventProducer().get_instance().send(
            config.KAFKA_TOPIC,
            value=event_data
        )


