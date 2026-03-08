from core.codegen.type.common.exception import TypedefValidationError
from core.codegen.type.common.model import GroupDef, TypeNode


class TypedefValidator:

    @classmethod
    def validate(cls, groups: list[GroupDef]) -> None:
        valid_refs = set()
        for group in groups:
            for msg in group.msg_defs:
                valid_refs.add(f"{group.group}.msg.{msg.msg_name}")
            for enum in group.enum_defs:
                valid_refs.add(f"{group.group}.enum.{enum.enum_name}")
            for const in group.constant_defs.constants:
                valid_refs.add(f"{group.group}.constant.{const.key.name}")
        
        for group in groups:
            for msg in group.msg_defs:
                for field in msg.fields:
                    cls._validate_type(field.key.type, valid_refs, group.group)
            for const in group.constant_defs.constants:
                cls._validate_type(const.key.type, valid_refs, group.group)
                
    @staticmethod
    def _validate_type(type_node: TypeNode, valid_refs: set[str], current_group: str) -> None:
        for part in type_node.type_parts:
            if part.is_primitive_type or part.is_iterable_type or part.is_typing_type or part.is_custom_type:
                continue
            if part.is_type_ref and part.type_ref_group != "base":
                group = part.type_ref_group or current_group
                ref_type = "msg" if part.is_msg_type_ref else "enum" if part.is_enum_type_ref else "constant"
                name = part.type_ref_name
                ref_key = f"{group}.{ref_type}.{name}"
                if ref_key not in valid_refs:
                    raise TypedefValidationError(
                        type_node.loc.file, 
                        type_node.loc.line, 
                        f"Unknown reference '{ref_key}' in type definition."
                    )
            elif not part.is_type_ref:
                raise TypedefValidationError(
                    type_node.loc.file, 
                    type_node.loc.line, 
                    f"Unknown type '{part.type_part_def}'"
                )