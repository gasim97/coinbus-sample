import os

from core.codegen.type.common.constant import PROJECT_DIR
from core.codegen.type.parsing.parser import TypedefParser
from core.codegen.type.rendering.renderer import TypedefRenderer
from core.codegen.type.validation.validator import TypedefValidator


class TypeGenerator:

    @staticmethod
    def _find_typedef_files(base_path: str) -> dict[str, str]:
        results = {}
        for root, _, files in os.walk(base_path):
            for file in files:
                if file.endswith(".typedef"):
                    group_name = file.split(".")[0]
                    results[group_name] = os.path.join(root, file)
        return results

    @classmethod
    def generate(cls) -> None:
        typedef_files = cls._find_typedef_files(PROJECT_DIR)
        groups = []
        for group, path in typedef_files.items():
            groups.append(TypedefParser.parse_group(group, path))

        TypedefValidator.validate(groups=groups)
        renderer = TypedefRenderer()
        renderer.render_groups(groups=groups)


if __name__ == "__main__":
    TypeGenerator.generate()