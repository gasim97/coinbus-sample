from abc import abstractmethod

from common.utils.pool import Poolable
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectcancelorder import RejectCancelOrderMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg


class Order(Poolable):

    @property
    @abstractmethod
    def is_complete(self) -> bool:
        ...

    @abstractmethod
    def on_enter_order(self, msg: EnterOrderMsg) -> None:
        ...

    @abstractmethod
    def on_order_entered(self, msg: OrderEnteredMsg) -> None:
        ...

    @abstractmethod
    def on_reject_enter_order(self, msg: RejectEnterOrderMsg) -> None:
        ...

    @abstractmethod
    def on_cancel_order(self, msg: CancelOrderMsg) -> None:
        ...

    @abstractmethod
    def on_order_cancelled(self, msg: OrderCancelledMsg) -> None:
        ...

    @abstractmethod
    def on_reject_cancel_order(self, msg: RejectCancelOrderMsg) -> None:
        ...

    @abstractmethod
    def on_fill(self, msg: FillMsg) -> None:
        ...

    @abstractmethod
    def on_order_completed(self, msg: OrderCompletedMsg) -> None:
        ...