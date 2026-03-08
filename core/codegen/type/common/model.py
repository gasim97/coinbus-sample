from __future__ import annotations

import re

from dataclasses import dataclass, field
from typing import Any, Optional

from core.codegen.type.common.constant import (
    CONSTANTS_SUBMODULE, 
    CUSTOM_TYPES_INFO, 
    ENUM_SUBMODULE, 
    ITERABLE_TYPES, 
    MSG_SUBMODULE, 
    OUTPUT_BASE_MODULE, 
    PRIMITIVE_TYPES, 
    TYPING_TYPES,
)


@dataclass
class Location:
    file: str
    line: int


@dataclass
class GroupDef:
    group: str
    msg_defs: list[MsgDef]
    enum_defs: list[EnumDef]
    constant_defs: ConstantDefs

    def __post_init__(self) -> None:
        for msg_def in self.msg_defs:
            msg_def.group_def = self

    @property
    def msg_group_name(self) -> str:
        return f"{self.group[0].upper()}{self.group[1:]}"

    @property
    def msg_group_name_upper(self) -> str:
        return self.group.upper()

    @property
    def msg_group_types_class_name(self) -> str:
        return f"{self.msg_group_name}MsgType"

    @property
    def msg_group_types_file_name(self) -> str:
        return f"{self.group}msgtype"

    @property
    def msgs(self) -> list[MsgDef]:
        return self.msg_defs


@dataclass
class ConstantDefs:
    constants: list[Field] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return len(self.constants) == 0


@dataclass
class EnumDef:
    enum_name: str
    group: str
    value_type: str
    values: list[EnumValue] = field(default_factory=list)
    loc: Location = None  # type: ignore


@dataclass
class EnumValue:
    key: str
    value: str
    loc: Location
    val_type: Optional[str] = None

    @property
    def value_literal(self) -> str:
        return f'"{self.value}"' if getattr(self, "val_type", "str") == "str" else self.value


@dataclass
class MsgDef:
    msg_name: str
    group_def: GroupDef = None  # type: ignore[assignment]
    parent_msg_def: Optional[TypeNode] = None
    fields: list[Field] = field(default_factory=list)
    template_type_hints: list[str] = field(default_factory=list)
    property_type_hints: list[str] = field(default_factory=list)
    type_hints: list[str] = field(default_factory=list)
    type_ref_imports: set[SubmoduleImport] = field(default_factory=set)
    loc: Location = None  # type: ignore

    @property
    def group(self) -> str:
        return self.group_def.group if self.group_def else "unknown"

    @property
    def parent_msg_type(self) -> str:
        return self.parent_msg_def.type if self.parent_msg_def is not None else "Msg"

    @property
    def group_types_class_name(self) -> str:
        return self.group_def.msg_group_types_class_name if self.group_def else "MsgType"

    @property
    def has_msg_ref_fields(self) -> bool:
        return any(f.has_msg_refs for f in self.fields)

    @property
    def msg_refs(self) -> set[str]:
        return {
            part.type_name
            for field in self.fields
            for part in field.key.type.type_parts
            if part.is_msg_type_ref
        }


@dataclass
class Field:
    key: Key
    value: Value
    loc: Location

    def __post_init__(self) -> None:
        self.key.type.type_parts[0].is_implicitly_optional = not self.value.has_value

    @property
    def name(self) -> str:
        return self.key.name

    @property
    def type_hint(self) -> str:
        return self.key.type.type

    @property
    def default_value_expr(self) -> str:
        return self.value.value

    @property
    def has_msg_refs(self) -> bool:
        return any(part.is_msg_type_ref for part in self.key.type.type_parts)


@dataclass
class Key:
    name: str
    type: TypeNode
    loc: Location


@dataclass
class Value:
    value: str
    type: TypeNode
    loc: Location

    @property
    def has_value(self) -> bool:
        return self.value != "None"


@dataclass
class TypeNode:
    type_def_type: str
    type_parts: list[TypePart]
    loc: Location

    @property
    def type(self) -> str:
        root_rendered = self.type_parts[0].rendered_type
        if self.type_parts[0].is_implicitly_optional and not self.type_parts[0].is_explicitly_optional:
             root_rendered = f"Optional[{root_rendered}]"
        elif self.type_parts[0].is_explicitly_optional:
             root_rendered = f"Optional[{root_rendered}]"
        return root_rendered

    @property
    def is_primitive_type(self) -> bool:
        return self.type_def_type in PRIMITIVE_TYPES

    @property
    def is_custom_type(self) -> bool:
        return self.type_def_type in CUSTOM_TYPES_INFO


@dataclass
class TypePart:
    type_part_def: str
    is_explicitly_optional: bool
    is_implicitly_optional: bool = False
    generics: list[TypePart] = field(default_factory=list)

    @property
    def is_optional(self) -> bool:
        return self.is_explicitly_optional or self.is_implicitly_optional

    @property
    def components(self) -> list[str]:
        return re.split(pattern=r'[:.]', string=self.type_part_def)

    @property
    def type_name(self) -> str:
        components = self.components
        type_name = components[-1]
        if not self.is_type_ref or components[0] == "base":
            return type_name
        if self.is_msg_type_ref:
            return f"{type_name}Msg"
        if self.is_enum_type_ref:
            return f"{type_name}Enum"
        if self.is_constant_type_ref:
            return type_name
        raise ValueError(f"Invalid field type part: {self.type_part_def}")

    @property
    def rendered_type(self) -> str:
        t_name = self.type_name
        if self.generics:
            inner = ", ".join(g.rendered_type for g in self.generics)
            return f"{t_name}[{inner}]"
        return t_name

    @property
    def type_ref_group(self) -> Optional[str]:
        components = self.components
        if len(components) == 3 or (len(components) == 2 and self.type_name == "constant"):
            return components[0]
        return None

    @property
    def type_ref_name(self) -> str:
        return self.components[-1]

    @property
    def is_type_ref(self) -> bool:
        return len(self.components) > 1 or self.type_part_def == "constant"

    @property
    def is_msg_type_ref(self) -> bool:
        return self.is_type_ref and len(self.components) > 1 and self.components[-2] == "msg"

    @property
    def is_enum_type_ref(self) -> bool:
        return self.is_type_ref and len(self.components) > 1 and self.components[-2] == "enum"

    @property
    def is_constant_type_ref(self) -> bool:
        return self.is_type_ref and self.components[-1] == "constant"

    @property
    def is_primitive_type(self) -> bool:
        return self.type_part_def in PRIMITIVE_TYPES

    @property
    def is_iterable_type(self) -> bool:
        return self.type_part_def in ITERABLE_TYPES

    @property
    def is_typing_type(self) -> bool:
        return self.type_part_def in TYPING_TYPES

    @property
    def is_custom_type(self) -> bool:
        return self.type_part_def in CUSTOM_TYPES_INFO

    @property
    def custom_type_info(self) -> Optional[dict[str, Any]]:
        if self.is_custom_type:
            return CUSTOM_TYPES_INFO[self.type_part_def]
        return None

    @property
    def has_type_import(self) -> bool:
        return self.is_type_ref or self.is_custom_type or any(g.has_type_import for g in self.generics)

    def type_ref_module(self, default_group: str) -> Optional[str]:
        group = self.type_ref_group or default_group
        if self.is_msg_type_ref:
            return f"{OUTPUT_BASE_MODULE}.{group}.{MSG_SUBMODULE}.{self.type_ref_name.lower()}"
        if self.is_enum_type_ref:
            return f"{OUTPUT_BASE_MODULE}.{group}.{ENUM_SUBMODULE}.{self.type_ref_name.lower()}"
        if self.is_constant_type_ref:
            return f"{OUTPUT_BASE_MODULE}.{group}.{CONSTANTS_SUBMODULE}"
        return None

    def _get_base_imports(self, default_group: str, value: Optional['Value'] = None) -> list[SubmoduleImport]:
        imports = []
        group = self.type_ref_group or default_group
        if self.is_type_ref:
            if group != "base":
                if self.is_constant_type_ref:
                    assert value is not None, "value is required for constant type ref imports"
                    imports.append(SubmoduleImport(module=str(self.type_ref_module(default_group)), submodules=[value.value]))
                else:
                    imports.append(SubmoduleImport(module=str(self.type_ref_module(default_group)), submodules=[self.type_name]))
        elif self.is_custom_type:
            imports.extend([
                SubmoduleImport(module=import_info["module"], submodules=[import_info["submodule"]])
                for import_info in dict(self.custom_type_info or {})["imports"]
            ])
        return imports

    def _get_generic_imports(self, default_group: str, value: Optional['Value'] = None) -> list[SubmoduleImport]:
        imports = []
        for child in self.generics:
            imports.extend(child.type_import(default_group=default_group, value=value))
        return imports

    def type_import(self, default_group: str, value: Optional['Value'] = None) -> list[SubmoduleImport]:
        imports = self._get_base_imports(default_group=default_group, value=value)
        imports.extend(self._get_generic_imports(default_group=default_group, value=value))
        return imports


@dataclass
class SubmoduleImport:
    module: str
    submodules: list[str] = field(default_factory=list)

    def render(self) -> str:
        self.submodules.sort()
        return f"from {self.module} import {', '.join(self.submodules)}"

    def __hash__(self) -> int:
        return hash((self.module, tuple(self.submodules)))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.module == other.module and self.submodules == other.submodules