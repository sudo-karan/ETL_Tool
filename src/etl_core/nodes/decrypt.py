"""decrypt: field-level decryption of record values.

Selected fields (dotted paths) in each record hold ciphertext produced with
AES-GCM or Fernet; this node replaces them with the decrypted plaintext. The
key is never in the pipeline JSON -- it is resolved from a ``secret_ref`` at
run time -- and shares the crypto layer (:mod:`etl_core.crypto`) that the
Phase 3 server uses to encrypt secrets at rest.

Input records are never mutated; each is deep-copied before its fields are
rewritten. Decrypted plaintext is data, so it flows on the output edge but is
never written to logs or errors; the key is redacted from both.
"""
from __future__ import annotations

import base64
import copy
import json
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..crypto import Algo, CryptoError, KeyEncoding, TokenEncoding, make_cipher
from ..errors import ErrorCategory
from ..paths import get_path, set_path
from .base import Node, NodeContext, NodeInputs, NodeOutputs, Records
from .registry import register_node

_MISSING = object()


class DecryptConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    algo: Algo
    secret_ref: str  # key material; resolved from the run's secrets
    fields: list[str] = Field(min_length=1)  # dotted paths to decrypt
    # aes-gcm only: how the key and the token strings are encoded.
    key_encoding: KeyEncoding = "base64"
    token_encoding: TokenEncoding = "base64"
    aad: str | None = None  # aes-gcm additional authenticated data (utf-8)
    # How to present the recovered plaintext.
    output: Literal["text", "json", "bytes_base64"] = "text"
    # A field absent from a record: fail the node, or leave the record as-is.
    on_missing: Literal["error", "skip"] = "error"


@register_node
class DecryptNode(Node):
    type_name: ClassVar[str] = "decrypt"
    config_model: ClassVar[type[BaseModel]] = DecryptConfig
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)

    @classmethod
    def required_input_ports(cls, config: BaseModel) -> tuple[str, ...]:
        return ("in",)

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: DecryptConfig = self.config  # type: ignore[assignment]
        key_material = ctx.get_secret(cfg.secret_ref)
        try:
            cipher = make_cipher(
                cfg.algo,
                key_material,
                key_encoding=cfg.key_encoding,
                token_encoding=cfg.token_encoding,
                aad=cfg.aad.encode("utf-8") if cfg.aad is not None else None,
            )
        except CryptoError as exc:
            raise ctx.error(ErrorCategory.DECRYPTION, str(exc)) from exc

        records = inputs["in"][0]
        out: Records = []
        decrypted_fields = 0
        for index, record in enumerate(records):
            new_record = copy.deepcopy(record)
            for field in cfg.fields:
                token = get_path(new_record, field, default=_MISSING)
                if token is _MISSING:
                    if cfg.on_missing == "skip":
                        continue
                    raise ctx.error(
                        ErrorCategory.DECRYPTION,
                        f"field {field!r} is missing from record {index}",
                        details={"field": field, "record_index": index},
                    )
                set_path(new_record, field, self._decrypt_value(cipher, field, token, index, cfg, ctx))
                decrypted_fields += 1
            out.append(new_record)

        ctx.info(f"decrypted {decrypted_fields} field value(s) across {len(out)} record(s)")
        return {"out": out}

    def _decrypt_value(
        self,
        cipher: Any,
        field: str,
        token: Any,
        index: int,
        cfg: DecryptConfig,
        ctx: NodeContext,
    ) -> Any:
        if not isinstance(token, str):
            raise ctx.error(
                ErrorCategory.DECRYPTION,
                f"field {field!r} in record {index} is {type(token).__name__}, "
                "not a string token to decrypt",
                details={"field": field, "record_index": index},
            )
        try:
            plaintext = cipher.decrypt(token)
        except CryptoError as exc:
            # exc carries no key/plaintext material; still redacted by ctx.error.
            raise ctx.error(
                ErrorCategory.DECRYPTION,
                f"field {field!r} in record {index}: {exc}",
                details={"field": field, "record_index": index},
            ) from exc

        if cfg.output == "bytes_base64":
            return base64.b64encode(plaintext).decode("ascii")
        try:
            text = plaintext.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ctx.error(
                ErrorCategory.DECRYPTION,
                f"field {field!r} in record {index}: plaintext is not valid UTF-8 "
                "(use output 'bytes_base64' for binary data)",
                details={"field": field, "record_index": index},
            ) from exc
        if cfg.output == "json":
            try:
                return json.loads(text)
            except ValueError as exc:
                raise ctx.error(
                    ErrorCategory.DECRYPTION,
                    f"field {field!r} in record {index}: decrypted text is not valid JSON: {exc}",
                    details={"field": field, "record_index": index},
                ) from exc
        return text
