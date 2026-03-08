from dataclasses import dataclass
from typing import override

from application.signal.filter.parameter.parameter import SignalFilterParameter
from generated.type.signal.enum.signalfiltertype import SignalFilterTypeEnum
from generated.type.signal.enum.signalparameterproperty import SignalParameterPropertyEnum
from generated.type.signal.msg.kalmanfilter import KalmanFilterMsg
from generated.type.signal.msg.signalfilter import SignalFilterMsg


KALMAN_FILTER_SUPPORTED_SIGNAL_PARAMETER_PROPERTIES = [
    SignalParameterPropertyEnum.ABSOLUTE, 
    SignalParameterPropertyEnum.RELATIVE_FRACTION,
]


@dataclass
class KalmanFilterParameter(SignalFilterParameter):
    measurement_noise: float
    measurement_noise_property: SignalParameterPropertyEnum
    process_noise: float
    process_noise_property: SignalParameterPropertyEnum

    @override
    @property
    def filter_type(self) -> SignalFilterTypeEnum:
        return SignalFilterTypeEnum.KALMAN
    
    @override
    @staticmethod
    def from_msg(signal_id: str, msg: SignalFilterMsg) -> SignalFilterParameter:
        if not isinstance(msg, KalmanFilterMsg):
            raise ValueError(f"Invalid message type for Kalman filter parameter: {type(msg)}")
        return KalmanFilterParameter(
            signal_id=signal_id, 
            measurement_noise=msg.measurement_noise, 
            measurement_noise_property=msg.measurement_noise_property, 
            process_noise=msg.process_noise, 
            process_noise_property=msg.process_noise_property
        )
    
    def __post_init__(self) -> None:
        if self.measurement_noise_property not in KALMAN_FILTER_SUPPORTED_SIGNAL_PARAMETER_PROPERTIES:
            raise ValueError(f"Unsupported measurement noise property for Kalman filter: {self.measurement_noise_property}")
        if self.process_noise_property not in KALMAN_FILTER_SUPPORTED_SIGNAL_PARAMETER_PROPERTIES:
            raise ValueError(f"Unsupported process noise property for Kalman filter: {self.process_noise_property}")
    
    def resolve_measurement_noise(self, measurement: float) -> float:
        return self._resolve_parameter_value(
            parameter_value=self.measurement_noise, parameter_property=self.measurement_noise_property, measurement=measurement,
        )

    def resolve_process_noise(self, measurement: float) -> float:
        return self._resolve_parameter_value(
            parameter_value=self.process_noise, parameter_property=self.process_noise_property, measurement=measurement,
        )
    
    def _resolve_parameter_value(
        self, parameter_value: float, parameter_property: SignalParameterPropertyEnum, measurement: float,
    ) -> float:
        if parameter_property == SignalParameterPropertyEnum.ABSOLUTE:
            return parameter_value
        if parameter_property == SignalParameterPropertyEnum.RELATIVE_FRACTION:
            return measurement * parameter_value

    @override
    def __hash__(self) -> int:
        return hash((super().__hash__(), self.measurement_noise, self.process_noise))

    @override
    def __eq__(self, other) -> bool:
        if not isinstance(other, KalmanFilterParameter):
            return False
        return (
            super().__eq__(other)
            and self.measurement_noise == other.measurement_noise
            and self.process_noise == other.process_noise
        )