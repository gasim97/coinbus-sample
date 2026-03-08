import re

from typing import Optional

from core.codegen.type.common.constant import MSG_SUBMODULE, OUTPUT_BASE_MODULE
from core.codegen.type.common.model import Field, MsgDef, SubmoduleImport, TypePart


class MsgTemplateHelper:

    @staticmethod
    def render_typing_imports(msg: MsgDef, include_property_hints: bool = True) -> str:
        type_hints = {*msg.template_type_hints, *msg.type_hints}
        if include_property_hints:
            type_hints.update(msg.property_type_hints)
        return SubmoduleImport(module="typing", submodules=sorted(type_hints)).render()

    @staticmethod
    def render_type_ref_imports(msg: MsgDef) -> str:
        assert msg.group_def is not None, f"Group def is not set for msg def {msg.msg_name}"
        group_msg_types_import = SubmoduleImport(
            module=(
                f"{OUTPUT_BASE_MODULE}.{msg.group_def.group}."
                f"{MSG_SUBMODULE}.{msg.group_def.msg_group_types_file_name}"
            ), 
            submodules=[msg.group_def.msg_group_types_class_name]
        )
        parent_msg_import = (
            [] 
            if msg.parent_msg_def is None 
            else msg.parent_msg_def.type_parts[0].type_import(default_group=msg.group_def.group)
        )
        submodule_imports = [group_msg_types_import, *parent_msg_import, *msg.type_ref_imports]
        rendered_ref_imports = sorted({sm_import.render() for sm_import in submodule_imports})
        return "\n".join(rendered_ref_imports)

    @staticmethod
    def render_to_dict_value(field: Field) -> str:
        return f"self._{field.key.name}"

    @classmethod
    def render_copy_statement(cls, field: Field, value_accessor: str) -> str:
        return cls.render_type_part_copy_statement(
            type_part=field.key.type.type_parts[0], 
            value_accessor=value_accessor
        )

    @classmethod
    def render_type_part_copy_statement(
        cls, 
        type_part: TypePart, 
        value_accessor: Optional[str], 
        depth: int = 0
    ) -> str:
        opt_postfix = f" if {value_accessor} is not None else None" if type_part.is_optional else ""
        
        if type_part.is_msg_type_ref:
            return (
                f"self.create_{cls.render_msg_ref_as_snake_case(type_part.type_name)}(type=type({value_accessor}))"
                f".copy_from(other={value_accessor}){opt_postfix}"
            )
            
        if type_part.is_custom_type:
            custom_init = type_part.custom_type_info['cloning'](value_accessor)  # type: ignore
            return f"{custom_init}{opt_postfix}"
            
        if type_part.is_iterable_type and type_part.type_name == "list" and type_part.generics:
            item_var = f"item_{depth}"
            inner_logic = cls.render_type_part_copy_statement(
                type_part=type_part.generics[0],
                value_accessor=item_var, 
                depth=depth + 1
            )
            return f"[{inner_logic} for {item_var} in {value_accessor}]{opt_postfix}"

        return f"{value_accessor}"

    @classmethod
    def render_from_dict_value(cls, field: Field, value_accessor: str) -> str:
        type_ignore = "  # type: ignore[assignment, misc, type-var]" if field.has_msg_refs else ""
        return (
            f"{cls.render_type_part_initialisation(
                type_part=field.key.type.type_parts[0], 
                value_accessor=value_accessor
            )}{type_ignore}"
        )

    @classmethod
    def render_type_part_initialisation(
        cls, 
        type_part: TypePart, 
        value_accessor: Optional[str], 
        depth: int = 0
    ) -> str:
        opt_postfix = f" if {value_accessor} is not None else None" if type_part.is_optional else ""
        
        if type_part.is_msg_type_ref:
            return (
                f"self.create_{cls.render_msg_ref_as_snake_case(type_part.type_name)}"
                f"(type=msg_refs_lookup({value_accessor}['group'], {value_accessor}['type']))"
                f".from_dict(value={value_accessor}, msg_refs_lookup=msg_refs_lookup){opt_postfix}"
            )
            
        if type_part.is_enum_type_ref:
            return f"{type_part.type_name}[{value_accessor}]{opt_postfix}"
            
        if type_part.is_custom_type:
            custom_init = type_part.custom_type_info['initialisation'](value_accessor)  # type: ignore
            return f"{custom_init}{opt_postfix}"
            
        if type_part.is_iterable_type and type_part.type_name == "list" and type_part.generics:
            item_var = f"item_{depth}"
            inner_logic = cls.render_type_part_initialisation(
                type_part=type_part.generics[0],
                value_accessor=item_var, 
                depth=depth + 1
            )
            return f"[{inner_logic} for {item_var} in {value_accessor}]{opt_postfix}"
            
        return f"{value_accessor}"

    @staticmethod
    def render_msg_ref_as_snake_case(msg_ref: str) -> str:
        return "_".join(re.sub(r'(?<!^)(?=[A-Z])', '_', msg_ref).lower().split("_"))

    @classmethod
    def render_msg_ref_type_var_name(cls, msg_ref: str) -> str:
        return f"{cls.render_msg_ref_as_snake_case(msg_ref).upper()}_TYPE"
