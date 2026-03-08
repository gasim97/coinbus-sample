from typing import Any, Optional

from common.utils.time import millis_to_nanos
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.marketdata.msg.trade import TradeMsg


VENUE = VenueEnum.BINANCE
_BINANCE_DEPTH_UPDATE_MSG_TYPE = 'depthUpdate'
_BINANCE_TRADE_MSG_TYPE = 'trade'


_MAX_DEPTH_UPDATE_MSG_LEVELS_PER_SIDE = 100


def side_prices(order_book_side: list[list[str]]) -> list[float]:
    return [float(level[0]) for level in order_book_side]


def side_volumes(order_book_side: list[list[str]]) -> list[float]:
    return [float(level[1]) for level in order_book_side]


def get_depth_update_chunks_count(depth_update: dict[str, Any]) -> Optional[int]:
    """Returns the number of chunks needed for this depth update, or None if invalid"""
    if depth_update.get('e') != _BINANCE_DEPTH_UPDATE_MSG_TYPE:
        return None
    num_bid_chunks = (len(depth_update['b']) + _MAX_DEPTH_UPDATE_MSG_LEVELS_PER_SIDE - 1) // _MAX_DEPTH_UPDATE_MSG_LEVELS_PER_SIDE
    num_ask_chunks = (len(depth_update['a']) + _MAX_DEPTH_UPDATE_MSG_LEVELS_PER_SIDE - 1) // _MAX_DEPTH_UPDATE_MSG_LEVELS_PER_SIDE
    return max(num_bid_chunks, num_ask_chunks)


def _get_depth_update_side_chunk(side: list[list[str]], chunk_index: int) -> list[list[str]]:
    side_length = len(side)
    start_index = chunk_index * _MAX_DEPTH_UPDATE_MSG_LEVELS_PER_SIDE
    end_index = min(start_index + _MAX_DEPTH_UPDATE_MSG_LEVELS_PER_SIDE, side_length)
    return side[start_index:end_index] if start_index < side_length else []


def depth_update_stream(
    depth_update: dict[str, Any], 
    chunk_index: int, 
    num_chunks: int, 
    depth_update_msg: Optional[DepthUpdateMsg] = None,
) -> Optional[DepthUpdateMsg]:
    """Returns the populated depth update message for the specified chunk if valid, None if invalid"""
    if depth_update.get('e') != _BINANCE_DEPTH_UPDATE_MSG_TYPE:
        return None
    bids = _get_depth_update_side_chunk(side=depth_update['b'], chunk_index=chunk_index)
    asks = _get_depth_update_side_chunk(side=depth_update['a'], chunk_index=chunk_index)
    depth_update_msg = depth_update_msg or DepthUpdateMsg()
    depth_update_msg.venue = VENUE
    depth_update_msg.symbol = SymbolEnum[depth_update['s']]
    depth_update_msg.bid_prices = side_prices(bids)
    depth_update_msg.bid_volumes = side_volumes(bids)
    depth_update_msg.ask_prices = side_prices(asks)
    depth_update_msg.ask_volumes = side_volumes(asks)
    depth_update_msg.is_final_chunk = chunk_index == num_chunks - 1
    return depth_update_msg


def trade_stream(trade: dict[str, Any], trade_msg: Optional[TradeMsg] = None) -> Optional[TradeMsg]:
    """Returns the populated trade message for the specified trade if valid, None if invalid"""
    if trade.get('e') != _BINANCE_TRADE_MSG_TYPE:
        return None
    trade_msg = trade_msg or TradeMsg()
    trade_msg.venue = VENUE
    trade_msg.symbol = SymbolEnum[trade['s']]
    trade_msg.id = trade['t']
    trade_msg.price_str = trade['p']
    trade_msg.price = float(trade['p'])
    trade_msg.quantity_str = trade['q']
    trade_msg.quantity = float(trade['q'])
    trade_msg.time = millis_to_nanos(trade['T'])
    trade_msg.is_buyer_maker = trade['m']
    trade_msg.is_best_match = None
    return trade_msg