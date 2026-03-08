from typing import Callable, Optional, TypeVar, cast

from application.signal.signal.parameter.parameter import SignalParameter
from common.utils.list import none_filter
from generated.type.signal.enum.signalparametertype import SignalParameterTypeEnum
from generated.type.signal.msg.bookdepthsignalparameter import BookDepthSignalParameterMsg
from generated.type.signal.msg.decayfactorsignalparameter import DecayFactorSignalParameterMsg
from generated.type.signal.msg.signalparameter import SignalParameterMsg
from generated.type.signal.msg.windownanossignalparameter import WindowNanosSignalParameterMsg


P = TypeVar('P', bound=SignalParameterMsg)


class SignalParameterMsgMapper:

    _SIGNAL_PARAMETER_MSG_TO_MAPPER: dict[type[SignalParameterMsg], Callable[[P], SignalParameter]] = {
        WindowNanosSignalParameterMsg: lambda parameter: SignalParameter(
            parameter_type=SignalParameterTypeEnum.WINDOW_NS,
            value=cast(WindowNanosSignalParameterMsg, parameter).window,
        ),
        DecayFactorSignalParameterMsg: lambda parameter: SignalParameter(
            parameter_type=SignalParameterTypeEnum.DECAY_FACTOR,
            value=cast(DecayFactorSignalParameterMsg, parameter).decay_factor,
        ),
        BookDepthSignalParameterMsg: lambda parameter: SignalParameter(
            parameter_type=SignalParameterTypeEnum.BOOK_DEPTH,
            value=cast(BookDepthSignalParameterMsg, parameter).depth,
        ),
    }

    @classmethod
    def map(cls, parameters: Optional[list[P]]) -> list[SignalParameter]:
        if parameters is None:
            return []
        return none_filter([cls._map(parameter) for parameter in parameters])

    @classmethod
    def _map(cls, parameter: P) -> Optional[SignalParameter]:
        mapper = cls._SIGNAL_PARAMETER_MSG_TO_MAPPER.get(type(parameter))
        if mapper is not None:
            return mapper(parameter)
        return None