# Repository

The `repo` package provides an abstract contract and implementations for artifact persistence with lazy-loading, partial saving abilities.

## Concepts

- **BaseArtifactRepo**: An abstract contract for persisting and loading `BaseArtifact` and its heavy parts (`ArtifactPart`).
- **FsJsonRepo**: A simple file-system-based repository using JSON files. Available on linux only.
- **SqliteRepo**: A more robust repository using SQLite for metadata and status management.

## Anatomy

```
└── repo
    ├── __init__.py
    ├── base.py     => Contract for defining a repository
    ├── json.py     => Filesystem json format repository implementation
    ├── README.md
    └── sqlite.py   => Sqlite implementation
```

## Generic BaseArtifactRepo

`BaseArtifactRepo` is generic and expects two generics: `ART` which is short for _artifact_ of type _`BaseArtifact`_, and `PartNameT` which is a _Literal_ of strings.
Typically, any `BaseArtifactRepo` instance is smart enough to type-hint returning, and expected types (`ART`). However, due to static type checking limitations, we can't dynamically infer artifact parts names.
For example, `repo.load(aid,include=[])`. You wouldn't get auto-completions on passing `include` param. So, make yourself a favor and define `PartNameT` generic once per artifact type and pass it to your repository to get full type-hinting.

## Parts validation

Due to `BaseArtifact.get_part_map`, now any `BaseArtifactRepo` can validate parts on saving and on loading.

## Parts and include

Artifacts in Tadween can be large (e.g., audio transcriptions, speaker diarization). To mitigate network overhead and memory usage, artifacts are split into:
- **Root**: Metadata, identity (aid), status.
- **Parts**: Large, lazy-loaded components.

The repository handles these parts atomically, allowing you to save or load only the data you need.

## Usage Example
*For examples, see [examples/repo.py](../../../examples/repo/README.md) (if available).*

## Methods
`BaseArtifactRepo` provides:
- `create`: Initialize a new artifact (Root only).
- `save`: Batch save the artifact (Root + optional parts).
- `load`: Batch load the artifact (Root + optional parts).
- `save_part`: Atomically save/overwrite a specific part.
- `load_part`: Load a specific heavy part.
- `exists`: Check if a root artifact record exists.
- `delete`: Remove an artifact and its parts.
