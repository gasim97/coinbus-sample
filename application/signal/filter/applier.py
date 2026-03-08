from application.signal.filter.filter import SignalFilter
from application.signal.filter.kalman import KalmanFilter
from application.signal.filter.parameter.parameter import SignalFilterParameter
from generated.type.signal.enum.signalfiltertype import SignalFilterTypeEnum


class SignalFiltersApplier:

    _SIGNAL_FILTERS: dict[SignalFilterTypeEnum, SignalFilter] = {
        SignalFilterTypeEnum.KALMAN: KalmanFilter(),
    }

    @classmethod
    def apply(cls, value: float, filter_parameters: list[SignalFilterParameter]) -> float:
        for parameter in filter_parameters:
            filter = cls._SIGNAL_FILTERS.get(parameter.filter_type)
            if filter is not None:
                value = filter.apply(value, parameter)
        return value