class TypedefParseError(Exception):

    def __init__(self, file_path: str, line_number: int, message: str) -> None:
        super().__init__(f"{file_path}:{line_number}: Parse Error: {message}")


class TypedefValidationError(Exception):

    def __init__(self, file_path: str, line_number: int, message: str) -> None:
        super().__init__(f"{file_path}:{line_number}: Validation Error: {message}")