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
        try:
            result = delete_order(event_data["order_id"])

            if result == 0:
                raise ValueError("La commande à annuler est introuvable.")

            event_data["event"] = "OrderCancelled"
            OrderEventProducer().get_instance().send(
                config.KAFKA_TOPIC,
                value=event_data
            )

        except Exception as e:
            event_data["error"] = str(e)
            self.logger.error(f"Erreur lors de l'annulation : {e}")
  
