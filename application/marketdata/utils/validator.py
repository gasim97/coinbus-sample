from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg


def is_depth_update_valid(msg: DepthUpdateMsg) -> bool:
    if msg.symbol is None:
        return False 
    bids_valid = len(msg.bid_prices) == len(msg.bid_volumes)
    asks_valid = len(msg.ask_prices) == len(msg.ask_volumes)    
    has_updates = len(msg.bid_prices) > 0 or len(msg.ask_prices) > 0
    return bids_valid and asks_valid and has_updates