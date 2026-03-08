from typing import cast, override

from application.marketdata.store.marketdatamanager import MarketDataManager
from application.signal.store.tradeledger import TradePriceSignalsTradeLedger
from common.utils.pool import ObjectPoolManager
from core.node.enginetimeprovider import EngineTimeProvider
from core.node.messagesender import MessageSender
from core.node.subscription import SubscriptionManager
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum


class TradePriceSignalsMarketDataManager(MarketDataManager):

    def __init__(
        self, 
        pool_manager: ObjectPoolManager,
        subscription_manager: SubscriptionManager, 
        message_sender: MessageSender, 
        engine_time_provider: EngineTimeProvider,
    ):
        super().__init__(
            pool_manager=pool_manager,
            subscription_manager=subscription_manager, 
            message_sender=message_sender, 
            engine_time_provider=engine_time_provider,
        )
    
    @override
    def trade_ledger(self, venue: VenueEnum, symbol: SymbolEnum) -> TradePriceSignalsTradeLedger:
        key = self.key_store.get(venue=venue, symbol=symbol)
        if key not in self._trade_ledgers:
            self._trade_ledgers[key] = TradePriceSignalsTradeLedger(
                venue=venue, 
                symbol=symbol, 
                engine_time_provider=self._engine_time_provider,
            )
        return cast(TradePriceSignalsTradeLedger, self._trade_ledgers[key])