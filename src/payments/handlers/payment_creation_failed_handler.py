"""
Handler: Payment Creation Failed
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from db import get_sqlalchemy_session
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from stocks.commands.write_stock import check_in_items_to_stock, update_stock_redis

class PaymentCreationFailedHandler(EventHandler):
    """Handles PaymentCreationFailed events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "PaymentCreationFailed"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        session = get_sqlalchemy_session()

        try:
            check_in_items_to_stock(session, event_data["order_items"])
            session.commit()

            update_stock_redis(event_data["order_items"], "+")
            event_data["event"] = "StockIncreased"

        except Exception as e:
            session.rollback()
            event_data["event"] = "StockDecreaseFailed"
            event_data["error"] = str(e)

        finally:
            session.close()

        OrderEventProducer().get_instance().send(
            config.KAFKA_TOPIC,
            value=event_data
        )
