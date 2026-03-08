from typing import Callable, Optional, Self

from application.marketdata.store.orderbook import StreamOrderBook
from application.marketdata.store.tradeledger import TradeLedger
from common.type.venuesymbolkey import VenueSymbolKey, VenueSymbolKeyStore
from common.utils.pool import ObjectPoolManager
from core.node.enginetimeprovider import EngineTimeProvider
from core.node.messagesender import MessageSender
from core.node.subscription import SubscriptionManager
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.marketdata.msg.cleardepthupdate import ClearDepthUpdateMsg
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.marketdata.msg.subscribedepthupdate import SubscribeDepthUpdateMsg
from generated.type.marketdata.msg.subscribetrade import SubscribeTradeMsg
from generated.type.marketdata.msg.trade import TradeMsg


class MarketDataManager:

    __slots__ = (
        '_pool_manager',
        '_subscription_manager',
        '_message_sender',
        '_engine_time_provider',
        '_key_store',
        '_order_books',
        '_trade_ledgers',
        '_depth_update_callbacks',
        '_clear_depth_update_callbacks',
        '_trade_callbacks',
        '_subscribed_symbols',
        '_subscribed_all_symbols',
        '_depth_update_subscribed',
        '_clear_depth_update_subscribed',
        '_trade_subscribed',
        '_depth_update_subscription_pending_symbols',
        '_trade_subscription_pending_symbols',
    )

    def __init__(
        self, 
        pool_manager: ObjectPoolManager,
        subscription_manager: SubscriptionManager, 
        message_sender: MessageSender,
        engine_time_provider: EngineTimeProvider,
    ):
        self._pool_manager = pool_manager
        self._subscription_manager = subscription_manager
        self._message_sender = message_sender
        self._engine_time_provider = engine_time_provider
        self._key_store = VenueSymbolKeyStore(pool_manager=pool_manager)
        self._order_books: dict[VenueSymbolKey, StreamOrderBook] = {}
        self._trade_ledgers: dict[VenueSymbolKey, TradeLedger] = {}
        self._depth_update_callbacks: list[Callable[[DepthUpdateMsg], None]] = []
        self._clear_depth_update_callbacks: list[Callable[[ClearDepthUpdateMsg], None]] = []
        self._trade_callbacks: list[Callable[[TradeMsg], None]] = []
        self._subscribed_symbols: set[VenueSymbolKey] = set()
        self._subscribed_all_symbols = False
        self._depth_update_subscribed = False
        self._clear_depth_update_subscribed = False
        self._trade_subscribed = False
        self._depth_update_subscription_pending_symbols: set[VenueSymbolKey] = set()
        self._trade_subscription_pending_symbols: set[VenueSymbolKey] = set()

    @property
    def key_store(self) -> VenueSymbolKeyStore:
        return self._key_store

    def _subscribe_depth_update_message(self, force: bool = False, venue: Optional[VenueEnum] = None, symbol: Optional[SymbolEnum] = None) -> None:
        has_callbacks = force or len(self._depth_update_callbacks) > 0
        if has_callbacks and not self._depth_update_subscribed:
            self._subscription_manager.subscribe(type=DepthUpdateMsg, callback=self._handle_depth_update)
            self._depth_update_subscribed = True
        if venue is not None and symbol is not None:
            self._depth_update_subscription_pending_symbols.add(self.key_store.get(venue=venue, symbol=symbol))
        if self._depth_update_subscribed:
            for key in self._depth_update_subscription_pending_symbols:
                self._send_subscribe_depth_update(symbol=key.symbol, venue=key.venue)
            self._depth_update_subscription_pending_symbols.clear()

    def _subscribe_clear_depth_update_message(self, force: bool = False) -> None:
        has_callbacks = force or len(self._clear_depth_update_callbacks) > 0
        if has_callbacks and not self._clear_depth_update_subscribed:
            self._subscription_manager.subscribe(type=ClearDepthUpdateMsg, callback=self._handle_clear_depth_update)
            self._clear_depth_update_subscribed = True

    def _subscribe_trade_message(self, force: bool = False, venue: Optional[VenueEnum] = None, symbol: Optional[SymbolEnum] = None) -> None:
        has_callbacks = force or len(self._trade_callbacks) > 0
        if has_callbacks and not self._trade_subscribed:
            self._subscription_manager.subscribe(type=TradeMsg, callback=self._handle_trade)
            self._trade_subscribed = True
        if venue is not None and symbol is not None:
            self._trade_subscription_pending_symbols.add(self.key_store.get(venue=venue, symbol=symbol))
        if self._trade_subscribed:
            for key in self._trade_subscription_pending_symbols:
                self._send_subscribe_trade(symbol=key.symbol, venue=key.venue)
            self._trade_subscription_pending_symbols.clear()

    def _handle_depth_update(self, msg: DepthUpdateMsg) -> None:
        if not self.is_subscribed_to_symbol(venue=msg.venue, symbol=msg.symbol):
            return
        self.order_book(venue=msg.venue, symbol=msg.symbol).on_depth_update(msg=msg)
        for callback in self._depth_update_callbacks:
            callback(msg)

    def _handle_clear_depth_update(self, msg: ClearDepthUpdateMsg) -> None:
        if not self.is_subscribed_to_symbol(venue=msg.venue, symbol=msg.symbol):
            return
        self.order_book(venue=msg.venue, symbol=msg.symbol).clear()
        for callback in self._clear_depth_update_callbacks:
            callback(msg)
    
    def _handle_trade(self, msg: TradeMsg) -> None:
        if not self.is_subscribed_to_symbol(venue=msg.venue, symbol=msg.symbol):
            return
        self.trade_ledger(venue=msg.venue, symbol=msg.symbol).on_trade(msg=msg)
        for callback in self._trade_callbacks:
            callback(msg)
    
    def _send_subscribe_depth_update(self, venue: VenueEnum, symbol: SymbolEnum) -> None:
        with self._message_sender.create(type=SubscribeDepthUpdateMsg) as subscribe_depth_update_msg:
            subscribe_depth_update_msg.venue = venue
            subscribe_depth_update_msg.symbol = symbol
            self._message_sender.send(msg=subscribe_depth_update_msg)
    
    def _send_subscribe_trade(self, venue: VenueEnum, symbol: SymbolEnum) -> None:
        with self._message_sender.create(type=SubscribeTradeMsg) as subscribe_trade_msg:
            subscribe_trade_msg.venue = venue
            subscribe_trade_msg.symbol = symbol
            self._message_sender.send(msg=subscribe_trade_msg)

    def is_subscribed_to_symbol(self, venue: VenueEnum, symbol: SymbolEnum) -> bool:
        return self._subscribed_all_symbols or self.key_store.get(venue=venue, symbol=symbol) in self._subscribed_symbols

    def subscribe_to_all_symbols(self) -> Self:
        self._subscribed_all_symbols = True
        self._subscribe_depth_update_message()
        self._subscribe_clear_depth_update_message()
        self._subscribe_trade_message()
        return self

    def subscribe_to_symbol(self, venue: VenueEnum, symbol: SymbolEnum) -> Self:
        key = self.key_store.get(venue=venue, symbol=symbol)
        if key in self._subscribed_symbols:
            return self
        self._subscribed_symbols.add(key)
        self._subscribe_depth_update_message(venue=venue, symbol=symbol)
        self._subscribe_clear_depth_update_message()
        self._subscribe_trade_message(venue=venue, symbol=symbol)
        return self

    def subscribe_depth_update(self, callback: Optional[Callable[[DepthUpdateMsg], None]] = None) -> Self:
        if callback is not None:
            self._depth_update_callbacks.append(callback)
        self._subscribe_depth_update_message(force=True)
        self._subscribe_clear_depth_update_message(force=True)
        return self
    
    def subscribe_clear_depth_update(self, callback: Optional[Callable[[ClearDepthUpdateMsg], None]] = None) -> Self:
        if callback is not None:
            self._clear_depth_update_callbacks.append(callback)
        self._subscribe_clear_depth_update_message(force=True)
        return self
    
    def subscribe_trade(self, callback: Optional[Callable[[TradeMsg], None]] = None) -> Self:
        if callback is not None:
            self._trade_callbacks.append(callback)
        self._subscribe_trade_message(force=True)
        return self

    def has_order_book(self, venue: VenueEnum, symbol: SymbolEnum) -> bool:
        return self.key_store.get(venue=venue, symbol=symbol) in self._order_books
    
    def has_trade_ledger(self, venue: VenueEnum, symbol: SymbolEnum) -> bool:
        return self.key_store.get(venue=venue, symbol=symbol) in self._trade_ledgers
    
    def order_book(self, venue: VenueEnum, symbol: SymbolEnum) -> StreamOrderBook:
        key = self.key_store.get(venue=venue, symbol=symbol)
        if key not in self._order_books:
            self._order_books[key] = StreamOrderBook(
                venue=venue, 
                symbol=symbol, 
                pool_manager=self._pool_manager,
            )
        return self._order_books[key]
    
    def trade_ledger(self, venue: VenueEnum, symbol: SymbolEnum) -> TradeLedger:
        key = self.key_store.get(venue=venue, symbol=symbol)
        if key not in self._trade_ledgers:
            self._trade_ledgers[key] = TradeLedger(
                venue=venue, 
                symbol=symbol, 
                engine_time_provider=self._engine_time_provider,
            )
        return self._trade_ledgers[key]