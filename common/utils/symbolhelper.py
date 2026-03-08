from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.ticker import TickerEnum


def is_base_asset(symbol: SymbolEnum | str, asset: TickerEnum | str) -> bool:
    symbol_str = symbol.value if isinstance(symbol, SymbolEnum) else symbol
    asset_str = asset.value if isinstance(asset, TickerEnum) else asset
    return symbol_str.startswith(asset_str)


def is_quote_asset(symbol: SymbolEnum | str, asset: TickerEnum | str) -> bool:
    symbol_str = symbol.value if isinstance(symbol, SymbolEnum) else symbol
    asset_str = asset.value if isinstance(asset, TickerEnum) else asset
    return symbol_str.endswith(asset_str)