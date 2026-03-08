import smtplib

from dataclasses import dataclass
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader
from typing import Optional, override
from os.path import dirname, join

from application.orderentry.manager.subscription import OrderSubscriptionManager, PartialOrderHandler
from application.orderentry.order.internalorder import InternalOrder
from common.utils.datetime import nanos_timestamp_to_datetime
from common.utils.env import get_env, get_env_as_int, require_env
from core.node.node import ReadWriteNode
from generated.type.account.msg.accountinfo import AccountInfoMsg
from generated.type.account.msg.accountinforequest import AccountInfoRequestMsg
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.notification.msg.emailnotifierconfig import EmailNotifierConfigMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg


@dataclass
class _SmtpConfig:
    host: str
    port: int
    sender: str
    password: str


class EmailNotifierNode(ReadWriteNode, PartialOrderHandler[InternalOrder]):
    """
    Node that sends email notifications

    Environment variables required:
    EMAIL_NOTIFIER_SENDER: Email address to send from
    EMAIL_NOTIFIER_PASSWORD: Email password

    Environment variables optional:
    EMAIL_NOTIFIER_SMTP_HOST: SMTP server host (default: smtp.gmail.com)
    EMAIL_NOTIFIER_SMTP_PORT: SMTP server port (default: 465)
    """

    _DEFAULT_SMTP_HOST = "smtp.gmail.com"
    _DEFAULT_SMTP_PORT = 465
    _EMAIL_SUBJECT_PREFIX = "COINBUS:"
    _TEMPLATE_DIR = join(dirname(__file__), "..", "email", "template")

    __slots__ = ('_order_subscription_manager', '_jinja_env', '_config', '_my_requests', '_smtp_config')

    def __init__(self, name: str = "EMAILNOTIFIER", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._order_subscription_manager = OrderSubscriptionManager(
            subscription_manager=super().subscription_manager, 
            order_handler=self, 
            order_pool=super().pool_manager.pool(type=InternalOrder),
        )
        self._jinja_env = Environment(loader=FileSystemLoader(self._TEMPLATE_DIR))
        self._config = self.pool_manager.pool(type=EmailNotifierConfigMsg).get()
        self._my_requests: set[str] = set()
        self._subscribe_to_config_messages()
        self._subscribe_to_messages()
    
    @override
    def on_activated(self) -> None:
        self._smtp_config = _SmtpConfig(
            host=get_env("EMAIL_NOTIFIER_SMTP_HOST", self._DEFAULT_SMTP_HOST) or self._DEFAULT_SMTP_HOST,
            port=get_env_as_int("EMAIL_NOTIFIER_SMTP_PORT", self._DEFAULT_SMTP_PORT) or self._DEFAULT_SMTP_PORT,
            sender=require_env("EMAIL_NOTIFIER_SENDER"),
            password=require_env("EMAIL_NOTIFIER_PASSWORD"),
        )

    @override
    def on_deactivated(self) -> None:
        ...

    def _subscribe_to_config_messages(self) -> None:
        super().subscription_manager.subscribe_config(
            type=EmailNotifierConfigMsg, callback=self._handle_email_notifier_config,
        )

    def _subscribe_to_messages(self) -> None:
        self._order_subscription_manager.subscribe_all()
        super().subscription_manager.subscribe(type=AccountInfoRequestMsg, callback=self._handle_account_info_request)
        super().subscription_manager.subscribe(type=AccountInfoMsg, callback=self._handle_account_info)

    def _handle_email_notifier_config(self, msg: EmailNotifierConfigMsg) -> None:
        self._config.copy_from(other=msg)

    def _handle_account_info_request(self, msg: AccountInfoRequestMsg) -> None:
        self._my_requests.add(msg.request_id)

    def _handle_account_info(self, msg: AccountInfoMsg) -> None:
        is_my_request = msg.request_id in self._my_requests
        if is_my_request and self._config.notify_on_account_info:
            self._my_requests.remove(msg.request_id)
            self._send_account_info_email(account_info=msg)

    @override
    def handle_order_entered(self, order: InternalOrder, msg: OrderEnteredMsg) -> None:
        if self._config.request_account_info_on_order_entered:
            self._send_account_info_request(venue=order.venue)
        if self._config.notify_on_order_entered:
            self._send_order_entered_email(order=order)

    @override
    def handle_reject_enter_order(self, order: InternalOrder, msg: RejectEnterOrderMsg) -> None:
        if self._config.request_account_info_on_order_rejected:
            self._send_account_info_request(venue=order.venue)
        if self._config.notify_on_order_rejected:
            self._send_order_rejected_email(order=order, reject_reason=msg.reason)

    @override
    def handle_order_cancelled(self, order: InternalOrder, msg: OrderCancelledMsg) -> None:
        if self._config.request_account_info_on_order_cancelled:
            self._send_account_info_request(venue=order.venue)
        if self._config.notify_on_order_cancelled:
            self._send_order_cancelled_email(order=order)

    @override
    def handle_order_completed(self, order: InternalOrder, msg: OrderCompletedMsg) -> None:
        if self._config.request_account_info_on_order_completed and order.executed_quantity > 0:
            self._send_account_info_request(venue=order.venue)
        if self._config.notify_on_order_completed:
            self._send_order_completed_email(order=order)

    def _send_account_info_email(self, account_info: AccountInfoMsg) -> None:
        subject = f"{account_info.venue.value} Account Balance Update"
        template = self._jinja_env.get_template("account_info_notification.html")
        body = template.render(
            venue=account_info.venue.value,
            balances=account_info.balances,
            sent_at=nanos_timestamp_to_datetime(timestamp=super().engine_time_provider.engine_time)
        )
        self._send_email(subject=subject, body=body, is_html=True)

    def _send_order_entered_email(self, order: InternalOrder) -> None:
        subject = f"Order Entered - {order.side.value} {order.symbol} {order.quantity}@{order.price}"
        template = self._jinja_env.get_template("order_entered_notification.html")
        body = template.render(
            order=order,
            sent_at=nanos_timestamp_to_datetime(timestamp=super().engine_time_provider.engine_time)
        )
        self._send_email(subject=subject, body=body, is_html=True)

    def _send_order_rejected_email(self, order: InternalOrder, reject_reason: Optional[str]) -> None:
        subject = f"Order Rejected - {order.side.value} {order.symbol} {order.quantity}@{order.price}"
        template = self._jinja_env.get_template("order_rejected_notification.html")
        body = template.render(
            order=order,
            reject_reason=reject_reason or "Unknown",
            sent_at=nanos_timestamp_to_datetime(timestamp=super().engine_time_provider.engine_time)
        )
        self._send_email(subject=subject, body=body, is_html=True)

    def _send_order_cancelled_email(self, order: InternalOrder) -> None:
        subject = f"Order Cancelled - {order.side.value} {order.symbol} {order.quantity}@{order.price}"
        template = self._jinja_env.get_template("order_cancelled_notification.html")
        body = template.render(
            order=order,
            sent_at=nanos_timestamp_to_datetime(timestamp=super().engine_time_provider.engine_time)
        )
        self._send_email(subject=subject, body=body, is_html=True)

    def _send_order_completed_email(self, order: InternalOrder) -> None:
        subject = f"Order Completed - {order.side.value} {order.symbol} {order.executed_quantity}@{order.average_execution_price}"
        template = self._jinja_env.get_template("order_completed_notification.html")
        body = template.render(
            order=order,
            sent_at=nanos_timestamp_to_datetime(timestamp=super().engine_time_provider.engine_time)
        )
        self._send_email(subject=subject, body=body, is_html=True)

    def _send_email(self, subject: str, body: str, is_html: bool = False) -> None:
        msg = MIMEText(body, 'html' if is_html else 'plain')
        msg["Subject"] = f"{self._EMAIL_SUBJECT_PREFIX} {subject}"
        msg["From"] = self._smtp_config.sender
        msg["To"] = ", ".join(self._config.recipients)
        with smtplib.SMTP_SSL(self._smtp_config.host, self._smtp_config.port) as smtp_server:
            smtp_server.login(self._smtp_config.sender, self._smtp_config.password)
            smtp_server.sendmail(self._smtp_config.sender, self._config.recipients, msg.as_string())

    def _send_account_info_request(self, venue: VenueEnum) -> None:
        with super().message_sender.create(type=AccountInfoRequestMsg) as account_info_request_msg:
            account_info_request_msg.venue = venue
            super().message_sender.send(msg=account_info_request_msg)