from binance import ThreadedWebsocketManager  # type: ignore[import-untyped]
from binance.client import Client  # type: ignore[import-untyped]
from typing import Optional

from common.utils.env import require_env
from generated.type.core.enum.environment import EnvironmentEnum


class Resources:
    """
    Resources class that holds the node resources

    Environment variables required:
    BINANCE_PROD_API_KEY: Binance production API key
    BINANCE_PROD_API_SECRET: Binance production API secret
    BINANCE_TEST_API_KEY: Binance testnet API key
    BINANCE_TEST_API_SECRET: Binance testnet API secret
    """

    _BINANCE_API_CREDS = {
        EnvironmentEnum.PROD: lambda: dict(
            api_key=require_env("BINANCE_PROD_API_KEY"),
            api_secret=require_env("BINANCE_PROD_API_SECRET"),
        ),
        EnvironmentEnum.TEST: lambda: dict(
            api_key=require_env("BINANCE_TEST_API_KEY"),
            api_secret=require_env("BINANCE_TEST_API_SECRET"),
        ),
    }

    __slots__ = ('_environment', '_client', '_threaded_web_socket_manager')

    def __init__(self, environment: EnvironmentEnum):
        self._environment: EnvironmentEnum = environment
        self._client: Optional[Client] = None
        self._threaded_web_socket_manager: Optional[ThreadedWebsocketManager] = None
    
    @property
    def binance_client(self) -> Client:
        if self._client is None:
            self._client = Client(
                tld="com", 
                testnet=self._environment != EnvironmentEnum.PROD,
                **self._BINANCE_API_CREDS[self._environment](),
            )
        return self._client
    
    @property
    def binance_threaded_web_socket_manager(self) -> ThreadedWebsocketManager:
        if self._threaded_web_socket_manager is None:
            self._threaded_web_socket_manager = ThreadedWebsocketManager(
                tld="com", 
                testnet=self._environment != EnvironmentEnum.PROD,
                **self._BINANCE_API_CREDS[self._environment](),
            )
        return self._threaded_web_socket_manager