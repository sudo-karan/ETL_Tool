"""Connectivity diagnostics endpoint: runs the engine's test_connection with the
deployment SSRF policy and the caller's own encrypted secrets."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from etl_core.crypto import Cipher
from etl_core.diagnostics import DiagnosticReport, test_connection
from etl_core.errors import SecretNotFoundError
from etl_core.secrets import collect_refs_from_config

from ..config import Settings
from ..deps import get_cipher, get_current_user, get_session, get_settings
from ..models import User
from ..schemas import ConnectionTestRequest
from ..secrets_store import DbSecretsProvider

router = APIRouter(tags=["diagnostics"])


@router.post("/test-connection", response_model=DiagnosticReport)
async def run_test_connection(
    payload: ConnectionTestRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
    cipher: Cipher = Depends(get_cipher),
) -> DiagnosticReport:
    provider = DbSecretsProvider(session, user.id, cipher)
    secrets: dict[str, str] = {}
    for ref in collect_refs_from_config(payload.config):
        try:
            secrets[ref] = await provider.get(ref)
        except SecretNotFoundError:
            pass  # the tester reports the missing credential on its own rung
    return await test_connection(
        {"type": payload.type, "config": payload.config},
        secrets,
        ssrf_policy=settings.ssrf_policy(),
    )
