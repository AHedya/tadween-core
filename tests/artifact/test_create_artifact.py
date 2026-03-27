import enum
from typing import Literal
from uuid import UUID

import numpy as np
import pytest
from pydantic import BaseModel, Field

from tadween_core.types.artifact import (
    ArtifactPart,
    BaseArtifact,
    PicklePart,
    RootModel,
)
from tadween_core.types.artifact.base import (
    _inner_type,
    _non_none_args,
)


class SimpleRoot(RootModel):
    name: str = "test"
    score: int = 0


class TextPart(ArtifactPart):
    content: str = ""


class MetaPart(ArtifactPart):
    tags: list[str] = Field(default_factory=list)


class EagerDetails(BaseModel):
    description: str = "n/a"


class FullArtifact(BaseArtifact):
    root: SimpleRoot
    details: EagerDetails
    text: TextPart
    meta: MetaPart


class MinimalArtifact(BaseArtifact):
    root: SimpleRoot


class DataPart(PicklePart):
    value: int = 0
    name: str = ""


class NumpyPart(PicklePart):
    data: np.ndarray


class _CustomData:
    def __init__(self, x):
        self.x = x


class CustomPart(PicklePart):
    obj: _CustomData


class TestRootModel:
    def test_auto_uuid(self):
        r = RootModel()
        UUID(r.id)  # raises if not valid UUID

    def test_unique_ids(self):
        assert RootModel().id != RootModel().id

    def test_custom_id(self):
        r = RootModel(id="my-id")
        assert r.id == "my-id"

    def test_subclass_extra_fields(self):
        r = SimpleRoot(name="Alice", score=42)
        assert r.name == "Alice"
        assert r.score == 42


class TestArtifactPart:
    def test_instantiation(self):
        p = TextPart(content="hello")
        assert p.content == "hello"

    def test_arbitrary_types_allowed(self):
        class CustomPart(ArtifactPart):
            data: object = None

        cp = CustomPart(data=object())
        assert cp.data is not None

    def test_serialize_deserialize_roundtrip(self):
        p = TextPart(content="hello world")
        data = p.serialize()
        restored = TextPart.deserialize(data)
        assert restored.content == "hello world"

    def test_serialize_deserialize_with_list(self):
        p = MetaPart(tags=["a", "b", "c"])
        data = p.serialize()
        restored = MetaPart.deserialize(data)
        assert restored.tags == ["a", "b", "c"]


class TestPicklePart:
    def test_instantiation(self):
        p = DataPart(value=42)
        assert p.value == 42

    def test_serialize_deserialize_roundtrip(self):
        p = DataPart(value=42, name="test")
        data = p.serialize()
        restored = DataPart.deserialize(data)
        assert restored.value == 42
        assert restored.name == "test"

    def test_handles_numpy_array_without_custom_validator(self):
        arr = np.array([1.0, 2.0, 3.0])
        p = NumpyPart(data=arr)
        data = p.serialize()
        restored = NumpyPart.deserialize(data)
        assert np.array_equal(restored.data, arr)

    def test_handles_custom_object(self):
        cd = _CustomData(x=99)
        p = CustomPart(obj=cd)
        data = p.serialize()
        restored = CustomPart.deserialize(data)
        assert restored.obj.x == 99


class TestBaseArtifactValidDefinitions:
    def test_valid_root_model(self):
        class GoodRoot(RootModel):
            stage: Literal["hello", "world"] = "hello"

    def test_minimal_artifact_creates(self):
        a = MinimalArtifact(root=SimpleRoot())
        assert a.root is not None

    def test_full_artifact_creates(self):
        a = FullArtifact(root=SimpleRoot(name="x"), details=EagerDetails())
        assert a.details.description == "n/a"

    def test_parts_are_auto_optional(self):
        """ArtifactPart fields must default to None without explicit value."""
        a = FullArtifact(root=SimpleRoot(), details=EagerDetails())
        assert a.text is None
        assert a.meta is None

    def test_parts_accept_value(self):
        a = FullArtifact(
            root=SimpleRoot(),
            details=EagerDetails(),
            text=TextPart(content="hi"),
        )
        assert a.text.content == "hi"

    def test_id_property_delegates_to_root(self):
        root = SimpleRoot(name="z")
        a = MinimalArtifact(root=root)
        assert a.id == root.id


class TestBaseArtifactInvalidDefinitions:
    def test_bad_root_model(self):
        with pytest.raises(TypeError):

            class BadRoot(RootModel):
                stage: str | None

        with pytest.raises(TypeError):

            class BadRoot(RootModel):
                duration: str | int

        with pytest.raises(TypeError):

            class BadRoot(RootModel):
                root: Literal["hello", "world", 123] = "hello"

        with pytest.raises(TypeError):

            class StageEnum(enum.StrEnum):
                INIT = "init"
                FINISH = "finish"

            class BadRoot(RootModel):
                stage: StageEnum  # enums aren't yet supported

    def test_primitive_field_rejected(self):
        with pytest.raises(TypeError, match="Primitives belong inside RootModel"):

            class BadArtifact(BaseArtifact):
                root: SimpleRoot
                name: str  # primitive not allowed at artifact level

    def test_optional_syntax_rejected(self):
        with pytest.raises(TypeError):

            class BadArtifact(BaseArtifact):
                root: SimpleRoot
                details: EagerDetails | None

    def test_union_syntax_rejected(self):
        with pytest.raises(TypeError):

            class BadArtifact(BaseArtifact):
                root: SimpleRoot
                details: EagerDetails | None

    def test_multi_type_union_rejected(self):
        with pytest.raises(TypeError, match="union of multiple concrete types"):

            class BadArtifact(BaseArtifact):
                root: SimpleRoot
                details: EagerDetails | EagerDetails | TextPart

    def test_optional_root_rejected(self):
        with pytest.raises(TypeError, match="must not be optional"):

            class BadArtifact(BaseArtifact):
                root: SimpleRoot | None

    def test_optional_eager_basemodel_rejected(self):
        with pytest.raises(
            TypeError, match="eager BaseModel fields must not be optional"
        ):

            class BadArtifact(BaseArtifact):
                root: SimpleRoot
                details: EagerDetails | None  # only ArtifactPart may be optional

    def test_subscripted_type_rejected(self):
        with pytest.raises(TypeError, match="unsupported subscripted type"):

            class BadArtifact(BaseArtifact):
                root: SimpleRoot
                tags: list[str]


class TestMaps:
    def test_part_map_contains_parts(self):
        pm = FullArtifact.get_part_map()
        assert "text" in pm and pm["text"] is TextPart
        assert "meta" in pm and pm["meta"] is MetaPart

    def test_part_map_excludes_root_and_eager(self):
        pm = FullArtifact.get_part_map()
        assert "root" not in pm
        assert "details" not in pm

    def test_eager_map_contains_eager(self):
        em = FullArtifact.get_eager_map()
        assert "details" in em and em["details"] is EagerDetails

    def test_eager_map_excludes_parts_and_root(self):
        em = FullArtifact.get_eager_map()
        assert "root" not in em
        assert "text" not in em
        assert "meta" not in em

    def test_minimal_artifact_maps_empty(self):
        assert MinimalArtifact.get_part_map() == {}
        assert MinimalArtifact.get_eager_map() == {}


class TestHelpers:
    def test_non_none_args_plain_type(self):
        assert _non_none_args(int) is None

    def test_non_none_args_pipe_union(self):
        result = _non_none_args(int | None)
        assert result == [int]

    def test_non_none_args_multi_pipe(self):
        result = _non_none_args(int | str | None)
        assert set(result) == {int, str}

    def test_inner_type_plain(self):
        assert _inner_type(TextPart) is TextPart

    def test_inner_type_unwraps_optional(self):
        assert _inner_type(TextPart | None) is TextPart
