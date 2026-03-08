import logging

from application.filelogger.node.filteredtextfilelogger import FilteredTextFileLoggerNode
from application.marketdata.node.binancestreamclient import BinanceMarketDataStreamClientNode
from application.orderentry.node.binanceclient import BinanceOrderEntryClientNode
from application.orderentry.node.simulator import OrderEntrySimulatorNode
from application.orderentry.node.testordergenerator import TestOrderGeneratorNode
from application.referencedata.node.binanceclient import BinanceReferenceDataClientNode
from common.utils.time import minutes_to_seconds
from core.engine.launcher import Launcher
from generated.type.core.enum.environment import EnvironmentEnum


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)


def test_order_entry():
    launcher = Launcher(
        environment=EnvironmentEnum.TEST,
        node_klasses=dict(
            FILTEREDFILELOGGER=FilteredTextFileLoggerNode,
            MDSTREAMCLIENT=BinanceMarketDataStreamClientNode,
            REFERENCEDATACLIENT=BinanceReferenceDataClientNode,
            # ORDERENTRYCLIENT=BinanceOrderEntryClientNode,
            ORDERENTRYCLIENTSIMULATOR=OrderEntrySimulatorNode,
            TESTORDERGENERATOR=TestOrderGeneratorNode,
        ),
    )
    try:
        launcher.start(run_duration_seconds=minutes_to_seconds(2))
    finally:
        launcher.stop()


if __name__ == "__main__":
    test_order_entry() 