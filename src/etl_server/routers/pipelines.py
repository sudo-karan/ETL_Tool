"""Per-user pipeline CRUD. The spec is validated against the engine schema."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_core.schema import PipelineSpec

from ..deps import get_current_user, get_session
from ..models import Pipeline, User
from ..schemas import PipelineCreate, PipelineRead, PipelineUpdate
from ..service import get_owned_pipeline

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


_UNPROCESSABLE = 422  # version-agnostic (Starlette renamed the 422 constant)


def _validate_spec(spec: dict) -> None:
    try:
        PipelineSpec.model_validate(spec)
    except ValidationError as exc:
        raise HTTPException(
            _UNPROCESSABLE, f"invalid pipeline spec: {exc.error_count()} error(s)"
        ) from exc


@router.post("", response_model=PipelineRead, status_code=status.HTTP_201_CREATED)
async def create_pipeline(
    payload: PipelineCreate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Pipeline:
    _validate_spec(payload.spec)
    pipeline = Pipeline(owner_id=user.id, name=payload.name, spec=payload.spec)
    session.add(pipeline)
    await session.commit()
    await session.refresh(pipeline)
    return pipeline


@router.get("", response_model=list[PipelineRead])
async def list_pipelines(
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[Pipeline]:
    result = await session.execute(
        select(Pipeline).where(Pipeline.owner_id == user.id).order_by(Pipeline.created_at.desc())
    )
    return list(result.scalars().all())


@router.get("/{pipeline_id}", response_model=PipelineRead)
async def get_pipeline(
    pipeline_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Pipeline:
    return await get_owned_pipeline(session, pipeline_id, user.id)


@router.put("/{pipeline_id}", response_model=PipelineRead)
async def update_pipeline(
    pipeline_id: uuid.UUID,
    payload: PipelineUpdate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Pipeline:
    pipeline = await get_owned_pipeline(session, pipeline_id, user.id)
    if payload.spec is not None:
        _validate_spec(payload.spec)
        pipeline.spec = payload.spec
    if payload.name is not None:
        pipeline.name = payload.name
    await session.commit()
    await session.refresh(pipeline)
    return pipeline


@router.delete("/{pipeline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pipeline(
    pipeline_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> None:
    pipeline = await get_owned_pipeline(session, pipeline_id, user.id)
    await session.delete(pipeline)
    await session.commit()
