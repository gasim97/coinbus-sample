from typing import Any, Optional

from common.type.wfloat import WFloat
from common.utils.number import decimal_places_from_string
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.referencedata.msg.symbolinfo import SymbolInfoMsg
from generated.type.referencedata.msg.symbolprice import SymbolPriceMsg


VENUE = VenueEnum.BINANCE


def _extract_symbol_info_filter(symbol_info: dict[str, Any], type: str) -> dict[str, Any]:
    filters = [filter for filter in symbol_info["filters"] if filter["filterType"] == type]
    if len(filters) == 1:
        return filters[0]
    raise ValueError(f"No {type} filter found in symbol info")


def symbol_info(
    symbol: SymbolEnum, 
    symbol_info: dict[str, Any],
    symbol_info_msg: Optional[SymbolInfoMsg] = None,
) -> SymbolInfoMsg:
    price_filter = _extract_symbol_info_filter(symbol_info=symbol_info, type="PRICE_FILTER")
    quantity_filter = _extract_symbol_info_filter(symbol_info=symbol_info, type="LOT_SIZE")
    market_quantity_filter = _extract_symbol_info_filter(symbol_info=symbol_info, type="MARKET_LOT_SIZE")
    notional_filter = _extract_symbol_info_filter(symbol_info=symbol_info, type="NOTIONAL")
    
    symbol_info_msg = symbol_info_msg or SymbolInfoMsg()
    symbol_info_msg.venue = VENUE
    symbol_info_msg.symbol = symbol
    symbol_info_msg.base_asset = TickerEnum[symbol_info["baseAsset"]]
    symbol_info_msg.quote_asset = TickerEnum[symbol_info["quoteAsset"]]

    symbol_info_msg.base_asset_precision = symbol_info["baseAssetPrecision"]
    symbol_info_msg.quote_asset_precision = symbol_info["quoteAssetPrecision"]
    symbol_info_msg.base_asset_commission_precision = symbol_info["baseCommissionPrecision"]
    symbol_info_msg.quote_asset_commission_precision = symbol_info["quoteCommissionPrecision"]

    price_msg = symbol_info_msg.create_numeric_value_info_msg()
    price_msg.min = float(price_filter["minPrice"])
    price_msg.max = float(price_filter["maxPrice"])
    price_msg.step_size = float(price_filter["tickSize"])
    price_msg.precision = decimal_places_from_string(value=price_filter["tickSize"])
    symbol_info_msg.price = price_msg

    quantity_msg = symbol_info_msg.create_numeric_value_info_msg()
    quantity_msg.min = float(quantity_filter["minQty"])
    quantity_msg.max = float(quantity_filter["maxQty"])
    quantity_msg.step_size = float(quantity_filter["stepSize"])
    quantity_msg.precision = decimal_places_from_string(value=quantity_filter["stepSize"])
    symbol_info_msg.quantity = quantity_msg

    market_quantity_msg = symbol_info_msg.create_numeric_value_info_msg()
    market_quantity_msg.min = float(market_quantity_filter["minQty"])
    market_quantity_msg.max = float(market_quantity_filter["maxQty"])
    market_quantity_msg.step_size = float(market_quantity_filter["stepSize"])
    market_quantity_msg.precision = decimal_places_from_string(value=market_quantity_filter["stepSize"])
    symbol_info_msg.market_quantity = market_quantity_msg

    notional_msg = symbol_info_msg.create_numeric_value_bounds_msg()
    notional_msg.min = float(notional_filter["minNotional"])
    notional_msg.max = float(notional_filter["maxNotional"])
    symbol_info_msg.notional = notional_msg

    return symbol_info_msg


def symbol_price(
    symbol: SymbolEnum, 
    symbol_price: dict[str, Any],
    symbol_price_msg: Optional[SymbolPriceMsg] = None,
) -> SymbolPriceMsg:
    if "price" not in symbol_price:
        raise ValueError("Price not found in symbol price")
    symbol_price_msg = symbol_price_msg or SymbolPriceMsg()
    symbol_price_msg.venue = VENUE
    symbol_price_msg.symbol = symbol
    symbol_price_msg.price = WFloat.from_string(value=symbol_price["price"])
    return symbol_price_msg