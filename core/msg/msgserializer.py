from msgpack import Packer, Unpacker  # type: ignore[import-untyped]
from typing import Any, Optional

from common.utils.pool import ObjectPoolManager
from core.msg.base.msg import Msg, MsgDictEncoder
from generated.type.msgrefs import MsgRefs


class MsgSerializer:

    _MSG_PACKER = Packer(default=MsgDictEncoder().default, use_bin_type=False, strict_types=False)
    _MSG_UNPACKER = Unpacker(raw=False)

    @classmethod
    def serialize(cls, msg: Msg) -> bytes:
        return cls._MSG_PACKER.pack(msg)

    @classmethod
    def deserialize(cls, serialized: bytes, pool_manager: ObjectPoolManager) -> Optional[Msg]:
        deserialized = cls.unpack(serialized)
        type_ref = MsgRefs.lookup_str(group=deserialized["group"], type=deserialized["type"])
        if type_ref is not None:
            return pool_manager.pool(type=type_ref).get().from_dict(
                value=deserialized, msg_refs_lookup=MsgRefs.lookup_str_strict,
            )
        return None
    
    @classmethod
    def unpack(cls, serialized: bytes) -> dict[str, Any]:
        cls._MSG_UNPACKER.feed(serialized)
        return next(cls._MSG_UNPACKER)