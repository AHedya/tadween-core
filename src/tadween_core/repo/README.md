# Repository

The `repo` package provides an abstract contract and implementations for artifact persistence with lazy-loading, partial saving abilities.

## Concepts

- **BaseArtifactRepo**: An abstract contract for persisting and loading `BaseArtifact` and its heavy parts (`ArtifactPart`).
- **S3Repo**: S3 repository interface.
- **FsJsonRepo**: A simple file-system-based repository using JSON files. Available on linux only.
- **SqliteRepo**: A more robust repository using SQLite.

## Anatomy

```
└── repo
    ├── __init__.py
    ├── base.py     => Contract for defining a repository
    ├── json.py     => Text-based filesystem in json format
    ├── README.md
    ├── s3.py       => S3 implementation
    └── sqlite.py   => Sqlite implementation
```

## Usage Example

*For examples, see [examples/repo.py](../../../examples/repo/README.md)*

## Parts and include

Artifacts can be large (e.g., audio transcriptions, speaker diarization). To mitigate network overhead and memory usage, artifacts are split into:
- **Root**: identity (aid), access, or filtration fields.
- **Eager**: always loaded parts. Expected to be small.
- **Parts**: Large, lazy-loaded components.

## Gotcha

***Important:*** As eager fields must be present with root, you will find any _repo_ uses the term _root_ to refer to both _root_ + _eager_. This distinction is important to understand the mechanism of flattening, building, serializing, and deserializing of an artifact.
---

*FsJsonRepo* is text-based and prioritize human-readability. This is a huge trade-off as you rarely find a human-readable heavy artifact part, and json format doesn't support bytes; it needs to be encoded to base64 which is relatively larger. 


### Storage Convention

Repos must persist root + eager fields as human-readable structured data (JSON, columns).
Parts are recommended to be stored as opaque bytes via `part.serialize()` / `PartType.deserialize(data)`.

This is a convention, not an enforcement. A repo may manage its internals as needed —
`FsJsonRepo` for example stores everything as UTF-8 text, including parts encoded as base64,
and is still a valid `BaseArtifactRepo` implementation. What matters is that the artifact
round-trips correctly through `save` and `load`.