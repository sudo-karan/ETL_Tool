"""$upstream / $iter reference resolution."""
from __future__ import annotations

import pytest

from etl_core.errors import ReferenceResolutionError
from etl_core.references import (
    IterContext,
    ReferenceContext,
    find_references,
    resolve_config,
)

UPSTREAM = {
    "n1": [{"id": 7, "user": {"name": "Ann", "tags": ["x", "y"]}}, {"id": 8}],
    "empty": [],
}


def ctx(iter_ctx=None):
    return ReferenceContext(upstream=UPSTREAM, iter=iter_ctx)


def test_whole_string_reference_preserves_type():
    assert resolve_config("$upstream.n1.id", ctx()) == 7
    assert isinstance(resolve_config("$upstream.n1.id", ctx()), int)


def test_whole_node_reference_returns_records():
    assert resolve_config("$upstream.n1", ctx()) == UPSTREAM["n1"]


def test_numeric_segment_picks_record_by_index():
    assert resolve_config("$upstream.n1.1.id", ctx()) == 8


def test_nested_path_and_list_index():
    assert resolve_config("$upstream.n1.user.tags.1", ctx()) == "y"


def test_embedded_template_interpolates():
    resolved = resolve_config("https://x.test/users/${upstream.n1.id}/posts", ctx())
    assert resolved == "https://x.test/users/7/posts"


def test_resolution_recurses_into_dicts_and_lists():
    raw = {"a": ["$upstream.n1.id", {"b": "${iter.value}!"}]}
    resolved = resolve_config(raw, ctx(IterContext(value="v", index=0)))
    assert resolved == {"a": [7, {"b": "v!"}]}


def test_iter_value_and_index():
    iter_ctx = IterContext(value={"id": 42}, index=3)
    assert resolve_config("$iter.value.id", ctx(iter_ctx)) == 42
    assert resolve_config("$iter.index", ctx(iter_ctx)) == 3


def test_iter_outside_scope_raises():
    with pytest.raises(ReferenceResolutionError, match="outside of an iterator"):
        resolve_config("$iter.value", ctx())


def test_unknown_upstream_node_raises():
    with pytest.raises(ReferenceResolutionError, match="no available output"):
        resolve_config("$upstream.ghost.id", ctx())


def test_empty_upstream_records_raise():
    with pytest.raises(ReferenceResolutionError, match="produced no records"):
        resolve_config("$upstream.empty.id", ctx())


def test_missing_path_raises():
    with pytest.raises(ReferenceResolutionError, match="not found"):
        resolve_config("$upstream.n1.nope", ctx())


def test_record_index_out_of_range():
    with pytest.raises(ReferenceResolutionError, match="out of range"):
        resolve_config("$upstream.n1.9.id", ctx())


def test_unknown_iter_field():
    with pytest.raises(ReferenceResolutionError, match="unknown \\$iter field"):
        resolve_config("$iter.nope", ctx(IterContext(value=1, index=0)))


def test_dollar_dollar_escapes_literal():
    assert resolve_config("$$upstream.n1.id", ctx()) == "$upstream.n1.id"


def test_plain_strings_untouched():
    assert resolve_config("$100 and iter.value", ctx()) == "$100 and iter.value"


def test_find_references():
    raw = {
        "url": "https://x.test/${upstream.a.id}",
        "params": {"v": "$iter.value", "w": "$$upstream.escaped"},
    }
    found = set(find_references(raw))
    assert found == {("upstream", "a.id"), ("iter", "value")}
