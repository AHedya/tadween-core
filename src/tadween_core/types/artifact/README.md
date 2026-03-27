# Artifact

The `artifact` package defines the core data model and its lazy-loaded parts.
Artifact fields can only be one of:`RootModel`, `BaseModel`, `BaseArtifactPart`.

| Field type | Format | Reason |
|---|---|---|
| `RootModel` | JSON/columns | Human-readable, quick access, filtration |
| `BaseModel` (eager) | JSON | Human-readable, always loaded with root |
| `BaseArtifactPart` (lazy) | bytes | Heavy data, loaded on demand |

## Storage Contract
_Root_ and _eager_ fields are human readable data, so they _should_ be json serializable as json dump is the default serialization for both _root_ and _eager_ fields (they are kept as a whole.)<br>
_Parts_ (the heavy ones) are expected to be serialized into bytes. Each `BaseArtifactPart` instance implements `serialize` and `deserialize` methods to dump serialized part into binary and vice versa (part -> model_dump -> serialize -> binary), (binary -> deserialise -> model_validate -> part).<br>

### Built-in Part Types

| Class | Format | Use Case |
|---|---|---|
| `ArtifactPart` | msgpack | Default; primitives and simple types. Or complex data types but make sure to serialize/validate them |
| `PicklePart` | pickle | Arbitrary Python objects (numpy arrays, custom classes) |

**SECURITY WARNING**: `PicklePart` can execute arbitrary code during deserialization. Only use with trusted data sources.

## Concepts

- **BaseArtifact**: An abstract contract for any artifact object. `Repo` builds upon it
- **RootModel**: `BaseModel` defines _artifact_ identity and quick access, filtration fields.
- **BaseArtifactPart**: Part contract. defines the serialization backend and binary format 

## Anatomy

```
└── artifact
    ├── __init__.py
    ├── base.py     => Defines `BaseArtifact` contract and validation.
    ├── part.py     => Defines `BaseArtifactPart` contract, implement `ArtifactPart` and some utility serializers/validators.
    └── README.md
```
## Lazy Loading 

Large data volumes are split into:
- **Root**: identity (aid), timestamps, status. is subclass of `RootModel`
- **Parts**: Large, lazy-loaded components, and automatically set to optional (None defaulted) so it doesn't cause problems to artifact model if not included. Subclass of `ArtifactPart`. All defaults on artifact level are omitted and all parts fields are set to None as a default value.
- **eager fields**: Parts that don't require optimization. Eagerly-loaded, must be subclass of `BaseModel`. 

This contract ensures minimal network overhead and memory usage when moving artifacts across different stages of a pipeline and also compatible with various types of repositories.

