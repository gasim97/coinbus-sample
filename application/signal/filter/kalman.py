from dataclasses import dataclass
from typing import Any, override

from common.utils.memo import Memo, MemoItem, with_memo
from application.signal.filter.filter import SignalFilter
from application.signal.filter.parameter.kalman import KalmanFilterParameter
from application.signal.filter.parameter.parameter import SignalFilterParameter
from generated.type.signal.enum.signalfiltertype import SignalFilterTypeEnum


@dataclass
class _KalmanFilterMemoItem(MemoItem):
    estimate: float = 0.0
    error_covariance: float = 1.0

    @override
    def update(self, estimate: float = 0.0, error_covariance: float = 1.0, **kwargs: Any) -> None:
        self.estimate = estimate
        self.error_covariance = error_covariance


class KalmanFilter(SignalFilter):

    def __init__(self) -> None:
        super().__init__(filter_type=SignalFilterTypeEnum.KALMAN)

    @override
    def apply(self, value: float, parameter: SignalFilterParameter) -> float:
        if not isinstance(parameter, KalmanFilterParameter):
            raise ValueError(f"Invalid parameter type for Kalman filter: {type(parameter)}")
        return self._apply(measurement=value, parameter=parameter)

    @with_memo(key_type=Any, item_type=_KalmanFilterMemoItem)
    def _apply(
        self, measurement: float, parameter: KalmanFilterParameter, memo: Memo[Any, _KalmanFilterMemoItem],
    ) -> float:
        """Applies a simple 1D Kalman filter to a new measurement"""
        memo_item = memo.get_or_create(key=parameter.signal_id, estimate=measurement, error_covariance=1.0)

        measurement_noise = parameter.resolve_measurement_noise(measurement=measurement)
        process_noise = parameter.resolve_process_noise(measurement=measurement)

        predicted_error_covariance = memo_item.error_covariance + process_noise
        kalman_gain = (
            predicted_error_covariance
            / (predicted_error_covariance + measurement_noise * measurement)
        )

        estimate = memo_item.estimate + kalman_gain * (measurement - memo_item.estimate)
        error_covariance = (1 - kalman_gain) * predicted_error_covariance
        memo_item.update(estimate=estimate, error_covariance=error_covariance)

        return estimate