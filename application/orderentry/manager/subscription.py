from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional, TypeVar, override

from application.orderentry.order.order import Order
from common.utils.pool import ObjectPool
from core.node.subscription import SubscriptionManager
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectcancelorder import RejectCancelOrderMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg


O = TypeVar('O', bound=Order)


class OrderFilter(Generic[O], ABC):

    @abstractmethod
    def included(self, order: O) -> bool:
        ...


class NoOpOrderFilter(OrderFilter[O]):

    @override
    def included(self, order: O) -> bool:
        return True


class LambdaOrderFilter(OrderFilter[O]):

    __slots__ = ('_func',)

    def __init__(self, func: Callable[[O], bool]):
        self._func = func
    
    @override
    def included(self, order: O) -> bool:
        return self._func(order)


class OrderSubscriptionManager(Generic[O]):

    __slots__ = (
        '_subscription_manager',
        '_order_handler',
        '_order_pool',
        '_orders',
        '_persist_completed_orders',
        '_order_filter',
    )

    def __init__(
        self, 
        subscription_manager: SubscriptionManager, 
        order_handler: OrderHandler[O], 
        order_pool: ObjectPool[O],
        persist_completed_orders: bool = True,
        order_filter: Optional[OrderFilter[O]] = None,
    ):
        self._subscription_manager = subscription_manager
        self._order_handler = order_handler
        self._order_pool = order_pool
        self._orders: dict[str, O] = {}
        self._persist_completed_orders = persist_completed_orders
        self._order_filter = order_filter or NoOpOrderFilter()

    @property
    def orders(self) -> dict[str, O]:
        return self._orders

    def get_order(self, internal_order_id: str) -> Optional[O]:
        return self._orders.get(internal_order_id)

    def remove_order(self, internal_order_id: str) -> None:
        self._order_pool.release(self._orders.pop(internal_order_id))
    
    def _remove_order_if_complete(self, internal_order_id: str) -> None:
        order = self.get_order(internal_order_id)
        if order is None or not order.is_complete:
            return
        self.remove_order(internal_order_id)
    
    def subscribe_mine(self) -> None:
        self._subscription_manager.subscribe_mine(type=EnterOrderMsg, callback=self._handle_enter_order)
        self._subscribe()
    
    def subscribe_all(self) -> None:
        self._subscription_manager.subscribe(type=EnterOrderMsg, callback=self._handle_enter_order)
        self._subscribe()
    
    def _subscribe(self) -> None:
        self._subscription_manager.subscribe(type=OrderEnteredMsg, callback=self._handle_order_entered)
        self._subscription_manager.subscribe(type=RejectEnterOrderMsg, callback=self._handle_reject_enter_order)
        self._subscription_manager.subscribe(type=CancelOrderMsg, callback=self._handle_cancel_order)
        self._subscription_manager.subscribe(type=OrderCancelledMsg, callback=self._handle_order_cancelled)
        self._subscription_manager.subscribe(type=RejectCancelOrderMsg, callback=self._handle_reject_cancel_order)
        self._subscription_manager.subscribe(type=FillMsg, callback=self._handle_fill)
        self._subscription_manager.subscribe(type=OrderCompletedMsg, callback=self._handle_order_completed)
    
    def _handle_enter_order(self, msg: EnterOrderMsg) -> None:
        assert msg.internal_order_id not in self._orders
        order = self._order_pool.get()
        order.on_enter_order(msg=msg)
        if not self._order_filter.included(order=order):
            self._order_pool.release(order)
            return
        self._orders[msg.internal_order_id] = order
        self._order_handler.handle_enter_order(order=order, msg=msg)
    
    def _handle_order_entered(self, msg: OrderEnteredMsg) -> None:
        order = self.get_order(msg.internal_order_id)
        if order is None:
            return
        order.on_order_entered(msg=msg)
        self._order_handler.handle_order_entered(order=order, msg=msg)
    
    def _handle_reject_enter_order(self, msg: RejectEnterOrderMsg) -> None:
        order = self.get_order(msg.internal_order_id)
        if order is None:
            return
        order.on_reject_enter_order(msg=msg)
        self._order_handler.handle_reject_enter_order(order=order, msg=msg)
        if not self._persist_completed_orders:
            self._remove_order_if_complete(msg.internal_order_id)
    
    def _handle_cancel_order(self, msg: CancelOrderMsg) -> None:
        order = self.get_order(msg.internal_order_id)
        if order is None:
            return
        order.on_cancel_order(msg=msg)
        self._order_handler.handle_cancel_order(order=order, msg=msg)
    
    def _handle_order_cancelled(self, msg: OrderCancelledMsg) -> None:
        order = self.get_order(msg.internal_order_id)
        if order is None:
            return
        order.on_order_cancelled(msg=msg)
        self._order_handler.handle_order_cancelled(order=order, msg=msg)
        if not self._persist_completed_orders:
            self._remove_order_if_complete(msg.internal_order_id)
    
    def _handle_reject_cancel_order(self, msg: RejectCancelOrderMsg) -> None:
        order = self.get_order(msg.internal_order_id)
        if order is None:
            return
        order.on_reject_cancel_order(msg=msg)
        self._order_handler.handle_reject_cancel_order(order=order, msg=msg)
    
    def _handle_fill(self, msg: FillMsg) -> None:
        order = self.get_order(msg.internal_order_id)
        if order is None:
            return
        order.on_fill(msg=msg)
        self._order_handler.handle_fill(order=order, msg=msg)
    
    def _handle_order_completed(self, msg: OrderCompletedMsg) -> None:
        order = self.get_order(msg.internal_order_id)
        if order is None:
            return
        order.on_order_completed(msg=msg)
        self._order_handler.handle_order_completed(order=order, msg=msg)
        if not self._persist_completed_orders:
            self._remove_order_if_complete(msg.internal_order_id)


class OrderHandler(Generic[O], ABC):

    @abstractmethod
    def handle_enter_order(self, order: O, msg: EnterOrderMsg) -> None:
        ...

    @abstractmethod
    def handle_order_entered(self, order: O, msg: OrderEnteredMsg) -> None:
        ...

    @abstractmethod
    def handle_reject_enter_order(self, order: O, msg: RejectEnterOrderMsg) -> None:
        ...

    @abstractmethod
    def handle_cancel_order(self, order: O, msg: CancelOrderMsg) -> None:
        ...

    @abstractmethod
    def handle_order_cancelled(self, order: O, msg: OrderCancelledMsg) -> None:
        ...

    @abstractmethod
    def handle_reject_cancel_order(self, order: O, msg: RejectCancelOrderMsg) -> None:
        ...

    @abstractmethod
    def handle_fill(self, order: O, msg: FillMsg) -> None:
        ...

    @abstractmethod
    def handle_order_completed(self, order: O, msg: OrderCompletedMsg) -> None:
        ...


class PartialOrderHandler(OrderHandler[O]):

    @override
    def handle_enter_order(self, order: O, msg: EnterOrderMsg) -> None:
        ...

    @override
    def handle_order_entered(self, order: O, msg: OrderEnteredMsg) -> None:
        ...

    @override
    def handle_reject_enter_order(self, order: O, msg: RejectEnterOrderMsg) -> None:
        ...

    @override
    def handle_cancel_order(self, order: O, msg: CancelOrderMsg) -> None:
        ...

    @override
    def handle_order_cancelled(self, order: O, msg: OrderCancelledMsg) -> None:
        ...

    @override
    def handle_reject_cancel_order(self, order: O, msg: RejectCancelOrderMsg) -> None:
        ...

    @override
    def handle_fill(self, order: O, msg: FillMsg) -> None:
        ...

    @override
    def handle_order_completed(self, order: O, msg: OrderCompletedMsg) -> None:
        ...