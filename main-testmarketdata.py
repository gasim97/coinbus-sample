import logging

from application.filelogger.node.filteredtextfilelogger import FilteredTextFileLoggerNode
from application.marketdata.node.binancestreamclient import BinanceMarketDataStreamClientNode
from application.marketdata.node.testconsumer import TestMarketDataConsumerNode
from common.utils.time import days_to_seconds
from core.engine.launcher import Launcher
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.marketdata.msg.testmarketdataconsumerconfig import TestMarketDataConsumerConfigMsg


logging.basicConfig(
  format="%(asctime)s %(levelname)s %(name)-8s %(message)s",
  level=logging.INFO,
  datefmt="%Y-%m-%d %H:%M:%S"
)


def test_market_data_consumer_config(venue: VenueEnum, symbols: list[SymbolEnum]) -> TestMarketDataConsumerConfigMsg:
    config = TestMarketDataConsumerConfigMsg()
    config.venue = venue
    config.symbols = symbols
    return config


def test_market_data_consumer():
    launcher = Launcher(
        environment=EnvironmentEnum.PROD,
        node_klasses=dict(
            TEXTFILELOGGER=FilteredTextFileLoggerNode,
            MDSTREAMCLIENT=BinanceMarketDataStreamClientNode,
            TESTMDCONSUMER=TestMarketDataConsumerNode,
        ),
        node_configuration_msgs=dict(
            TESTMDCONSUMER=[
                test_market_data_consumer_config(
                    venue=VenueEnum.BINANCE,
                    symbols=[
                        SymbolEnum.ADAUSDT,
                        SymbolEnum.LINKUSDT,
                        SymbolEnum.SOLUSDT,
                        SymbolEnum.XRPUSDT,
                    ],
                ),
            ],
        ),
    )
    try:
        launcher.start(run_duration_seconds=days_to_seconds(60))
    finally:
        launcher.stop()


if __name__ == "__main__":
   test_market_data_consumer()