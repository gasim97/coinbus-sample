import re

from typing import Optional

from core.codegen.type.common.exception import TypedefParseError
from core.codegen.type.common.model import (
    ConstantDefs, 
    EnumDef, 
    EnumValue, 
    Field, 
    GroupDef, 
    Key, 
    Location, 
    MsgDef, 
    TypeNode, 
    TypePart, 
    Value,
)


class TypedefParser:
    
    @classmethod
    def _split_and_parse_generics(cls, inner_content: str, part: TypePart) -> None:
        inner_parts = []
        bracket_level = 0
        current_part = []
        for char in inner_content:
            if char == '[':
                bracket_level += 1
                current_part.append(char)
            elif char == ']':
                bracket_level -= 1
                current_part.append(char)
            elif char == ',' and bracket_level == 0:
                inner_parts.append("".join(current_part))
                current_part = []
            else:
                current_part.append(char)
                
        if current_part:
            inner_parts.append("".join(current_part))
            
        for inner in inner_parts:
            part.generics.append(cls._parse_part(inner))

    @classmethod
    def _parse_part(cls, type_str: str, explicitly_optional: bool = False) -> TypePart:
        type_str = type_str.strip()
        match = re.match(pattern=r'^([^\[]+)\[(.*)\]$', string=type_str)
        if match:
            part_name = match.group(1).strip()
            inner_content = match.group(2).strip()
            if part_name == "Optional":
                return cls._parse_part(inner_content, explicitly_optional=True)
            part = TypePart(type_part_def=part_name, is_explicitly_optional=explicitly_optional)
            cls._split_and_parse_generics(inner_content=inner_content, part=part)
            return part
        return TypePart(type_part_def=type_str, is_explicitly_optional=explicitly_optional)

    @classmethod
    def _create_type(cls, type_def_type: str, loc: Location) -> TypeNode:
        root = cls._parse_part(type_def_type)
        
        def collect_parts(node: TypePart, acc: list[TypePart]) -> None:
            acc.append(node)
            for child in node.generics:
                collect_parts(node=child, acc=acc)
                
        type_parts: list[TypePart] = []
        collect_parts(node=root, acc=type_parts)
        return TypeNode(type_def_type=type_def_type, loc=loc, type_parts=type_parts)

    @staticmethod
    def _split_on_first_space_before_value_characters(text: str) -> list[str]:
        for i, char in enumerate(text):
            if char in ['"', "'", "[", "(", "{"]:
                return [text]
            elif char == ' ':
                return [text[:i], text[i+1:]]
        return [text]

    @staticmethod
    def _parse_default_value_ast(value: str, type_node: TypeNode) -> str:
        for type_part in type_node.type_parts:
            if type_part.is_type_ref:
                prefix = f"{type_part.type_ref_name}."
                if value.startswith(prefix):
                    return f"{type_part.type_name}.{value[len(prefix):]}"
        return value

    @classmethod
    def _create_value(cls, type_def_value: Optional[str], key: Key, loc: Location) -> Value:
        if type_def_value is None or type_def_value == "None":
            return Value(value="None", type=key.type, loc=loc)
            
        type_and_value_parts = cls._split_on_first_space_before_value_characters(type_def_value)
        if len(type_and_value_parts) == 1:
            type_and_value_parts.insert(0, key.type.type_def_type)
            
        type_node = cls._create_type(type_def_type=type_and_value_parts[0], loc=loc)
        value = cls._parse_default_value_ast(value=type_and_value_parts[1], type_node=type_node)
        return Value(value=value, type=type_node, loc=loc)

    @classmethod
    def _create_key(cls, type_def_key: str, loc: Location) -> Key:
        type_and_name_parts = type_def_key.strip().rsplit(maxsplit=1)
        if len(type_and_name_parts) < 2:
            raise TypedefParseError(loc.file, loc.line, "Field definition missing type or name")
        type_node = cls._create_type(type_def_type=type_and_name_parts[0], loc=loc)
        name = type_and_name_parts[1]
        return Key(name=name, type=type_node, loc=loc)

    @classmethod
    def _create_field(cls, type_def_line: str, loc: Location) -> Field:
        key_value_parts = [item.strip() for item in type_def_line.split("=", 1)]
        key = cls._create_key(type_def_key=key_value_parts[0], loc=loc)
        value = cls._create_value(
            type_def_value=key_value_parts[1] if len(key_value_parts) > 1 else None, 
            key=key, 
            loc=loc
        )
        return Field(key=key, value=value, loc=loc)

    @classmethod
    def _parse_msg_definition(cls, line: str, loc: Location) -> MsgDef:
        match = re.match(
            pattern=r'^msg\s+([a-zA-Z0-9_]+)(?:\s*:\s*([a-zA-Z0-9_\.:\[\],]+))?(?:\s*(->))?$', 
            string=line
        )
        if not match:
            raise TypedefParseError(loc.file, loc.line, "Invalid msg definition syntax")
        msg_name = match.group(1)
        parent_type_str = match.group(2)
        parent_msg_def = cls._create_type(type_def_type=parent_type_str, loc=loc) if parent_type_str else None
        
        msg = MsgDef(msg_name=msg_name, parent_msg_def=parent_msg_def, loc=loc)
        msg.template_type_hints = ["override"]
        msg.property_type_hints = ["Any", "Callable", "Self", "Type", "TypeVar", "overload"]
        return msg

    @staticmethod
    def _parse_enum_definition(line: str, loc: Location, group: str) -> EnumDef:
        match = re.match(pattern=r'^enum\s+([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+)(?:\s*(->))?$', string=line)
        if not match:
            raise TypedefParseError(loc.file, loc.line, "Invalid enum definition syntax")
        value_type = match.group(1)
        enum_name = match.group(2)
        return EnumDef(enum_name=enum_name, group=group, value_type=value_type, loc=loc)

    @classmethod
    def _add_field_to_msg(cls, msg_def: MsgDef, field_line: str, group: str, loc: Location) -> None:
        field = cls._create_field(type_def_line=field_line, loc=loc)
        msg_def.fields.append(field)
        
        msg_def.type_hints.extend([
            type_part.type_name 
            for type_part in field.key.type.type_parts 
            if type_part.is_typing_type
        ])
        if any(type_part.is_optional for type_part in field.key.type.type_parts):
            msg_def.type_hints.append("Optional")
        for type_part in field.key.type.type_parts:
            if type_part.has_type_import:
                msg_def.type_ref_imports.update(type_part.type_import(default_group=group))
        for type_part in field.value.type.type_parts:
            if type_part.has_type_import:
                msg_def.type_ref_imports.update(type_part.type_import(default_group=group, value=field.value))

    @staticmethod
    def _add_value_to_enum(enum_def: EnumDef, value_line: str, loc: Location) -> None:
        key = value_line.split("=")[0].strip()
        value = " ".join(value_line.split("=")[1:]).strip()
        enum_def.values.append(EnumValue(key=key, value=value, loc=loc, val_type=enum_def.value_type))

    @staticmethod
    def parse_group(group: str, file_path: str) -> GroupDef:
        parser_state = _GroupParser(group=group.lower())
        
        with open(file_path, mode="r") as file_handle:
            for line_index, line in enumerate(file_handle):
                line_number = line_index + 1
                loc = Location(file_path, line_number)
                line = line.split("#")[0].strip()
                parts = line.split()
                
                if not parts:
                    continue
                
                is_header = parser_state.process_header(parts=parts, line=line, loc=loc)
                if not is_header:
                    parser_state.process_body_line(line=line, loc=loc)
                    
        return parser_state.finalize()


class _GroupParser:

    def __init__(self, group: str) -> None:
        self.group = group
        self.msg_defs: list[MsgDef] = []
        self.enum_defs: list[EnumDef] = []
        self.constant_defs = ConstantDefs()
        self.current_msg: Optional[MsgDef] = None
        self.current_enum: Optional[EnumDef] = None

    def _close_current_blocks(self) -> None:
        if self.current_msg:
            self.msg_defs.append(self.current_msg)
            self.current_msg = None
        if self.current_enum:
            self.enum_defs.append(self.current_enum)
            self.current_enum = None

    def process_header(self, parts: list[str], line: str, loc: Location) -> bool:
        if parts[0] == "msg":
            self._close_current_blocks()
            self.current_msg = TypedefParser._parse_msg_definition(line=line, loc=loc)
            return True
        if parts[0] == "enum":
            self._close_current_blocks()
            self.current_enum = TypedefParser._parse_enum_definition(line=line, loc=loc, group=self.group)
            return True
        if parts[0] == "constant":
            self._close_current_blocks()
            constant = TypedefParser._create_field(type_def_line=" ".join(parts[1:]), loc=loc)
            self.constant_defs.constants.append(constant)
            return True
        return False

    def process_body_line(self, line: str, loc: Location) -> None:
        if self.current_msg:
            TypedefParser._add_field_to_msg(msg_def=self.current_msg, field_line=line, group=self.group, loc=loc)
        elif self.current_enum:
            TypedefParser._add_value_to_enum(enum_def=self.current_enum, value_line=line, loc=loc)
        else:
            raise TypedefParseError(loc.file, loc.line, "Invalid syntax outside of block")

    def finalize(self) -> GroupDef:
        self._close_current_blocks()
        return GroupDef(
            group=self.group, 
            msg_defs=self.msg_defs, 
            enum_defs=self.enum_defs, 
            constant_defs=self.constant_defs
        )