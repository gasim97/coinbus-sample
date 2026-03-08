from typing import Optional, TypeVar

from application.signal.filter.kalman import KalmanFilterParameter
from application.signal.filter.parameter.parameter import SignalFilterParameter
from common.utils.list import none_filter
from generated.type.signal.msg.kalmanfilter import KalmanFilterMsg
from generated.type.signal.msg.signalfilter import SignalFilterMsg


F = TypeVar('F', bound=SignalFilterMsg)


class SignalFilterMsgParameterMapper:
    
    _SIGNAL_FILTER_MSG_TYPE_TO_PARAMETER_TYPE: dict[type[SignalFilterMsg], type[SignalFilterParameter]] = {
        KalmanFilterMsg: KalmanFilterParameter,
    }

    @classmethod
    def map(cls, signal_id: str, filters: Optional[list[F]]) -> list[SignalFilterParameter]:
        if filters is None:
            return []
        return none_filter([cls._map(signal_id=signal_id, filter=filter) for filter in filters])

    @classmethod
    def _map(cls, signal_id: str, filter: F) -> Optional[SignalFilterParameter]:
        parameter_type = cls._SIGNAL_FILTER_MSG_TYPE_TO_PARAMETER_TYPE.get(type(filter))
        if parameter_type is not None:
            return parameter_type.from_msg(signal_id=signal_id, msg=filter)
        return None