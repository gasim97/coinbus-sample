from common.type.wfloat import WFloat
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.referencedata.msg.symbolinfo import SymbolInfoMsg


class Commission:

    _DEFAULT_COMMISSION_RATE = 0.001

    @staticmethod
    def calculate(
        symbol_info: SymbolInfoMsg, 
        side: SideEnum, 
        base_quantity: WFloat, 
        quote_quantity: WFloat,
        commission_rate: float = _DEFAULT_COMMISSION_RATE,
    ) -> tuple[TickerEnum, WFloat]:
        if side == SideEnum.BUY:
            commission_asset = symbol_info.base_asset
            commission = WFloat.round(
                value=base_quantity.as_float * commission_rate, precision=symbol_info.base_asset_commission_precision,
            )
        else:
            commission_asset = symbol_info.quote_asset
            commission = WFloat.round(
                value=quote_quantity.as_float * commission_rate, precision=symbol_info.quote_asset_commission_precision,
            )
        return commission_asset, commission
