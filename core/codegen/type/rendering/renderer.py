import os
import shutil

from jinja2 import Environment, FileSystemLoader

from core.codegen.type.common.constant import (
    CONSTANTS_SUBMODULE, 
    MSG_REFS_SUBMODULE,
    MSG_SUBMODULE, 
    OUTPUT_BASE_MODULE, 
    OUTPUT_BASE_PATH, 
    TEMPLATE_PATHS, 
)
from core.codegen.type.common.model import GroupDef, SubmoduleImport
from core.codegen.type.rendering.templatehelper import MsgTemplateHelper


class TypedefRenderer:

    def __init__(self) -> None:
        self.jinja2_env = Environment(loader=FileSystemLoader(TEMPLATE_PATHS))
        # Expose helpers directly to Jinja environment
        self.jinja2_env.globals['MsgTemplateHelper'] = MsgTemplateHelper
        
    def _write_file(self, subdir: str, name: str, content: str) -> None:
        dir_path = os.path.join(OUTPUT_BASE_PATH, subdir)
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, name)
        with open(f"{file_path}.py", mode="w") as file_handle:
            file_handle.writelines(content)

    def _extract_constant_imports(self, group: GroupDef) -> tuple[str, str]:
        constants = group.constant_defs.constants
        type_hints = set(
            part.type_name for const in constants 
            for part in [*const.key.type.type_parts, *const.value.type.type_parts] if part.is_typing_type
        )
        typing_imports = SubmoduleImport(module="typing", submodules=sorted(type_hints)).render() + "\n" if type_hints else ""
        
        ref_import_set = set(
            type_ref_import for const in constants 
            for part in [*const.key.type.type_parts, *const.value.type.type_parts] if part.has_type_import
            for type_ref_import in part.type_import(default_group=group.group, value=const.value)
        )
        ref_imports = "\n".join(sorted(imp.render() for imp in ref_import_set)) + "\n\n" if ref_import_set else ""
        return typing_imports, ref_imports

    def _render_constants(self, group: GroupDef) -> None:
        if not group.constant_defs.is_empty:
            typing_imports, ref_imports = self._extract_constant_imports(group=group)
            content = self.jinja2_env.get_template("constants.py.jinja2").render(
                typing_imports=typing_imports,
                ref_imports=ref_imports,
                constants=group.constant_defs.constants
            )
            self._write_file(group.group, CONSTANTS_SUBMODULE, content)

    def _extract_group_type_refs(self, group_def: GroupDef, type_imports: list[str], type_refs: list[str]) -> None:
        for msg in group_def.msg_defs:
            type_imports.append(SubmoduleImport(
                module=f"{OUTPUT_BASE_MODULE}.{group_def.group}.{MSG_SUBMODULE}.{msg.msg_name.lower()}",
                submodules=[f"{msg.msg_name}Msg"]
            ).render())
            type_refs.append(
                f"    _MsgRef("
                f"group=MsgGroup.{group_def.msg_group_name.upper()}, "
                f"type={group_def.msg_group_types_class_name}.{msg.msg_name.upper()}, "
                f"ref={msg.msg_name}Msg"
                f"),"
            )
    
        if group_def.msg_defs:
            type_imports.append(SubmoduleImport(
                module=(
                    f"{OUTPUT_BASE_MODULE}.{group_def.group}."
                    f"{MSG_SUBMODULE}.{group_def.msg_group_types_file_name}"
                ),
                submodules=[group_def.msg_group_types_class_name]
            ).render())

    def _render_type_refs(self, groups: list[GroupDef]) -> None:
        if not groups:
            return
            
        type_imports: list[str] = []
        type_refs: list[str] = []
        for group_def in groups:
            self._extract_group_type_refs(group_def=group_def, type_imports=type_imports, type_refs=type_refs)
            
        content = self.jinja2_env.get_template("msgrefs.py.jinja2").render(
            type_refs="\n".join(sorted(type_refs)),
            type_imports="\n".join(sorted(type_imports))
        )
        self._write_file("", MSG_REFS_SUBMODULE, content)

    def _render_msgs(self, group: GroupDef) -> None:
        if group.msg_defs:
            content = self.jinja2_env.get_template("msg_group_types.py.jinja2").render(
                msg_group_types_class_name=group.msg_group_types_class_name,
                msgs=group.msg_defs
            )
            self._write_file(f"{group.group}/msg", group.msg_group_types_file_name, content)
        
        for msg in group.msg_defs:
            template_name = "msg.py.jinja2" if msg.fields else "no_property_msg.py.jinja2"
            content = self.jinja2_env.get_template(template_name).render(msg=msg)
            self._write_file(f"{group.group}/msg", msg.msg_name.lower(), content)

    def _render_enums(self, group: GroupDef) -> None:
        for enum in group.enum_defs:
            content = self.jinja2_env.get_template("enum.py.jinja2").render(enum=enum)
            self._write_file(f"{group.group}/enum", enum.enum_name.lower(), content)

    def render_groups(self, groups: list[GroupDef]) -> None:
        shutil.rmtree(OUTPUT_BASE_PATH, ignore_errors=True)
        
        for group in groups:
            self._render_msgs(group=group)
            self._render_enums(group=group)
            self._render_constants(group=group)
                
        # Type Refs
        self._render_type_refs(groups=groups)