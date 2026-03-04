# Repository

The `repo` package provides an abstract contract and implementations for artifact persistence with lazy-loading, partial saving abilities.

## Concepts

- **BaseArtifactRepo**: An abstract contract for persisting and loading `BaseArtifact` and its heavy parts (`ArtifactPart`).
- **FsJsonRepo**: A simple file-system-based repository using JSON files. Available on linux only.
- **SqliteRepo**: A more robust repository using SQLite.

## Anatomy

```
└── repo
    ├── __init__.py
    ├── base.py     => Contract for defining a repository
    ├── json.py     => Filesystem json format repository implementation
    ├── README.md
    └── sqlite.py   => Sqlite implementation
```

## Usage Example

*For examples, see [examples/repo.py](../../../examples/repo/README.md)*



## Parts and include

Artifacts can be large (e.g., audio transcriptions, speaker diarization). To mitigate network overhead and memory usage, artifacts are split into:
- **Root**: identity (aid), access, or filtration fields.
- **Eager**: always loaded parts. Expected to be small.
- **Parts**: Large, lazy-loaded components.
