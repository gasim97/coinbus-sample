def decimal_places_from_string(value: str, trim: bool = True) -> int:
    if "." not in value:
        return 0
    parts = value.split(".")
    if len(parts) != 2:
        raise RuntimeError(
            f"Attempted to get number of decimal places of string with multiple dots: {value}"
        )
    length = len(parts[1])
    if trim:
        while length > 0 and parts[1][length - 1] == '0':
            length -= 1
    return length


def decimal_places_from_float(value: float) -> int:
    return decimal_places_from_string(value=str(value), trim=True)