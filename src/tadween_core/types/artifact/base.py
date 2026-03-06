from abc import ABC
from types import UnionType
from typing import Any, Literal, Optional, Union, get_args, get_origin
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field
from pydantic.fields import FieldInfo

_PRIMITIVES: frozenset[type] = frozenset({str, int, float, bool})


class RootModel(BaseModel):
    """
    Identity, quick access and filtration data fields. The model must be flat, and use primitive data types.
    """

    id: str = Field(default_factory=lambda: str(uuid4()))

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        cls._validate_fields()

    # TODO: Add support for non-primitives
    @classmethod
    def _validate_fields(cls) -> None:
        for name, field_info in cls.model_fields.items():
            ann = field_info.annotation
            if get_origin(ann) is UnionType or get_origin(ann) is Optional:
                raise TypeError(
                    f"RootModel field '{name}' on '{cls.__name__}' must not be optional. "
                    f"Nullable filtration fields are not permitted. Remove '| None'."
                )
            _resolve_storage_type(ann)

    @classmethod
    def get_field_type_map(cls) -> dict[str, type]:
        return {
            name: _resolve_storage_type(field_info.annotation)
            for name, field_info in cls.model_fields.items()
        }


class ArtifactPart(BaseModel):
    """Base class for all heavy artifact parts.
    Inherit from this class to mark a field as a lazy-loaded part.

    You'd need to implement custom serialization/validation logic if you use arbitrary/custom types.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)


class BaseArtifact(ABC, BaseModel):
    """
    Strict contract for artifacts.

    Allowed fields types:
    - `RootModel`: A must
    - `ArtifactPart`: optional
    - `BaseModel`: optional

    Composes:
    - Exactly one RootModel     -> flat record (always loaded)
    - Any BaseModel fields      -> eagerly loaded. Never optional
    - Any ArtifactPart fields   -> lazy-loaded, auto-optional
    """

    root: RootModel

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        cls._inject_optional_parts()
        cls._validate_fields()

    @classmethod
    def _inject_optional_parts(cls) -> None:
        for name, field_info in list(cls.model_fields.items()):
            # guard against extra optional and defaulted part
            ann = _inner_type(field_info.annotation)
            if isinstance(ann, type) and issubclass(ann, ArtifactPart):
                cls.model_fields[name] = FieldInfo(
                    default=None,
                    annotation=ann | None,
                )
        cls.model_rebuild(force=True)

    @classmethod
    def _validate_fields(cls) -> None:
        """
        Validates that all artifact-level fields are one of:
        - RootModel subclass    (eager)
        - ArtifactPart subclass (lazy, auto-optional)
        - BaseModel subclass    (eager)
        Raw primitives at artifact level are rejected — they belong in RootModel.

        """
        for name, field_info in cls.model_fields.items():
            ann = field_info.annotation
            origin = get_origin(field_info.annotation)

            # reject other subscripted types (list[str], dict[str, int], …)
            if origin is not None and not (origin is Union or origin is UnionType):
                raise TypeError(
                    f"Field '{name}' on '{cls.__name__}': "
                    f"unsupported subscripted type '{origin}'."
                )

            non_none = _non_none_args(ann)
            if non_none is not None:
                if len(non_none) > 1:
                    raise TypeError(
                        f"Field '{name}' on '{cls.__name__}': "
                        f"union of multiple concrete types is not allowed. Got {non_none}."
                    )
                inner = non_none[0]
                is_optional = True
            else:
                inner = ann
                is_optional = False

            if not (isinstance(inner, type) and issubclass(inner, BaseModel)):
                raise TypeError(
                    f"Field '{name}' on '{cls.__name__}' must be a RootModel, "
                    f"ArtifactPart, or BaseModel subclass. "
                    f"Primitives belong inside RootModel. Got: {inner}"
                )

            if is_optional:
                if issubclass(inner, RootModel):
                    raise TypeError(
                        f"Root field on '{cls.__name__}' must not be optional. Remove '| None'."
                    )
                if not issubclass(inner, ArtifactPart):
                    # Plain eager BaseModel — must not be optional
                    raise TypeError(
                        f"Field '{name}' on '{cls.__name__}': "
                        f"eager BaseModel fields must not be optional. Remove '| None'."
                    )

    @classmethod
    def get_part_map(cls) -> dict[str, type[ArtifactPart]]:
        """Lazy-loaded fields (auto-optional)"""
        return {
            name: _inner_type(field.annotation)
            for name, field in cls.model_fields.items()
            if issubclass(_inner_type(field.annotation), ArtifactPart)
        }

    @classmethod
    def get_eager_map(cls) -> dict[str, type[BaseModel]]:
        """Eagerly loaded inline fields (not root, not parts)."""
        return {
            name: _inner_type(field.annotation)
            for name, field in cls.model_fields.items()
            if issubclass(_inner_type(field.annotation), BaseModel)
            and not issubclass(_inner_type(field.annotation), (ArtifactPart, RootModel))
        }

    @property
    def id(self) -> str | UUID:
        return self.root.id


def _non_none_args(annotation) -> list[type] | None:
    """
    If *annotation* is a pipe-union (`X | Y | None`), return the non-None members.
    Returns ``None`` when *annotation* is not a union at all.
    """
    if get_origin(annotation) is not UnionType:
        return None
    return [a for a in get_args(annotation) if a is not type(None)]


def _inner_type(annotation: Any) -> type:
    """Return the single inner type, unwrapping X | None if needed."""
    args = _non_none_args(annotation)
    return args[0] if args is not None else annotation


def _resolve_storage_type(annotation: Any) -> type:
    """
    Resolve a field annotation to its concrete storage primitive.

    Resolution order:
    1. Annotation is a plain primitive     → returned as-is
    2. Annotation is ``Literal[v1, ...]``  → validated homogeneous, returns arg type
    3. Anything else                       → raises ``TypeError`

    """
    if annotation in _PRIMITIVES:
        return annotation

    if get_origin(annotation) is Literal:
        args = get_args(annotation)
        types = {type(a) for a in args}
        if len(types) != 1:
            raise TypeError(
                f"Literal args must all share the same primitive type. "
                f"Got mixed types {types} in {annotation}."
            )
        (resolved,) = types
        if resolved not in _PRIMITIVES:
            raise TypeError(
                f"Literal values must be primitives ({_PRIMITIVES}). "
                f"Got {resolved} in {annotation}."
            )
        return resolved

    raise TypeError(
        f"RootModel fields must be primitives or Literal of primitives. "
        f"Got: {annotation!r}"
    )
