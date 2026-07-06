"""Per-user secrets CRUD. Values are encrypted at rest and never returned."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_core.crypto import Cipher

from ..deps import get_cipher, get_current_user, get_session
from ..models import Secret, User
from ..schemas import SecretCreate, SecretRead
from ..secrets_store import delete_secret, upsert_secret

router = APIRouter(prefix="/secrets", tags=["secrets"])


@router.post("", response_model=SecretRead, status_code=status.HTTP_201_CREATED)
async def set_secret(
    payload: SecretCreate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
    cipher: Cipher = Depends(get_cipher),
) -> Secret:
    row = await upsert_secret(session, user.id, payload.ref, payload.value, cipher)
    await session.commit()
    await session.refresh(row)
    return row


@router.get("", response_model=list[SecretRead])
async def list_secrets(
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[Secret]:
    result = await session.execute(
        select(Secret).where(Secret.owner_id == user.id).order_by(Secret.ref)
    )
    return list(result.scalars().all())


@router.delete("/{ref}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_secret(
    ref: str,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> None:
    deleted = await delete_secret(session, user.id, ref)
    await session.commit()
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "secret not found")
