import os


_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
CODEGEN_TYPE_DIR = os.path.dirname(_FILE_DIR)
PROJECT_DIR = os.path.abspath(os.path.join(CODEGEN_TYPE_DIR, '../../..'))
TEMPLATE_PATHS = [
    os.path.join(CODEGEN_TYPE_DIR, "templates"),
]


PRIMITIVE_TYPES = {'int', 'float', 'str', 'bool', 'bytes'}
ITERABLE_TYPES = {'list'}
TYPING_TYPES = {'Optional'}
CUSTOM_TYPES_INFO = dict(
    WFloat=dict(
        imports=[
            dict(submodule="WFloat", module=".".join(["common", "type", "wfloat"])),
        ],
        initialisation=lambda value: f"WFloat.from_string(value={value})",
        cloning=lambda value: f"{value}.clone()",
    ),
)


OUTPUT_BASE_PATH_COMPONENTS = ["generated", "type"]
OUTPUT_BASE_PATH = os.path.join(PROJECT_DIR, *OUTPUT_BASE_PATH_COMPONENTS)
OUTPUT_BASE_MODULE = ".".join(["generated", "type"])
MSG_SUBMODULE = "msg"
ENUM_SUBMODULE = "enum"
CONSTANTS_SUBMODULE = "constants"
MSG_REFS_SUBMODULE = "msgrefs"