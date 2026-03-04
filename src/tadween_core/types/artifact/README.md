# Artifact

The `artifact` package defines the core data model and its lazy-loaded parts.
Artifact fields can only be one of: `BaseModel`, `ArtifactPart`, `RootModel`.
This is a semantic to:
- offload serialization/validation to `pydantic.BaseModel`.
- Mark heavy, lazy-loaded parts by `ArtifactPart`.

Both of (`BaseModel`, `RootModel`) are eagerly loaded by the repository, while `ArtifactPart` must be specified by field name in `include`.

## Concepts

- **BaseArtifact**: An abstract contract for any artifact object. `Repo` builds upon it
- **RootModel**: `BaseModel` defines _artifact_ identity and quick access, filtration fields.
- **ArtifactPart**: A base class for all heavy artifact parts. Inherit from this class to mark a field as a optional (default to None) and lazy-loaded part.
- **TadweenArtifact**: The default artifact model. Battery-included.

## Anatomy

```
└── artifact
    ├── __init__.py
    ├── base.py     => Defines `BaseArtifact` and `ArtifactPart` contracts.
    ├── README.md
    └── tadween.py  => Defines `TadweenArtifact` and implements helpers.
```
## Lazy Loading 

Large data volumes are split into:
- **Root**: identity (aid), timestamps, status. is subclass of `RootModel`
- **Parts**: Large, lazy-loaded components, and automatically set to optional (None defaulted) so it doesn't cause problems to artifact model if not included. Subclass of `ArtifactPart`.
- **eager fields**: Parts that don't require optimization. Eagerly-loaded, must be subclass of `BaseModel`. 

This contract ensures minimal network overhead and memory usage when moving artifacts across different stages of a pipeline and also compatible with various types of repositories.

