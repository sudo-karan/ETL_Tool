"""FastAPI dependencies.

Per-app state (settings, database, queue, secrets cipher) lives on
``app.state`` and is read here, so tests construct an app with their own SQLite
database and in-memory queue and everything downstream follows.
"""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from etl_core.crypto import Cipher

from .config import Settings
from .db import Database
from .models import User
from .queue import JobQueue
from .security import TokenError, decode_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token", auto_error=False)

_UNAUTH = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="not authenticated",
    headers={"WWW-Authenticate": "Bearer"},
)


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_database(request: Request) -> Database:
    return request.app.state.db


def get_queue(request: Request) -> JobQueue:
    return request.app.state.queue


def get_cipher(request: Request) -> Cipher:
    return request.app.state.cipher


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    db: Database = request.app.state.db
    async with db.sessionmaker() as session:
        yield session


async def get_current_user(
    token: str | None = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> User:
    if not token:
        raise _UNAUTH
    try:
        payload = decode_token(
            token, secret=settings.jwt_secret, algorithms=[settings.jwt_algorithm]
        )
    except TokenError:
        raise _UNAUTH
    subject = payload.get("sub")
    if not subject:
        raise _UNAUTH
    try:
        user_id = uuid.UUID(str(subject))
    except ValueError:
        raise _UNAUTH
    user = await session.get(User, user_id)
    if user is None or not user.is_active:
        raise _UNAUTH
    return user
