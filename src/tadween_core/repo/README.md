# Repository

The `repo` package provides an abstract contract and implementations for artifact persistence in Tadween.

## Concepts

- **BaseArtifactRepo**: An abstract contract for persisting and loading `BaseArtifact` and its heavy parts (`ArtifactPart`).
- **TadweenArtifact**: The default artifact model with lazy-loaded parts (e.g., ASRResult, LLMResult).
- **JsonArtifactRepo**: A simple file-system-based repository using JSON files.
- **SqliteArtifactRepo**: A more robust repository using SQLite for metadata and status management.

## Heavy Artifact Parts

Artifacts in Tadween can be large (e.g., audio transcriptions, speaker diarization). To mitigate network overhead and memory usage, artifacts are split into:
- **Root**: Metadata, identity (aid), status.
- **Parts**: Large, lazy-loaded components.

The repository handles these parts atomically, allowing you to save or load only the data you need.

## Usage Example

```python
from tadween_core.repo import JsonArtifactRepo
from tadween_core.types.artifact import TadweenArtifact, ASRResults

# 1. Initialize repository
repo = JsonArtifactRepo(base_dir="/tmp/artifacts")

# 2. Create an artifact
artifact = TadweenArtifact(id="artifact-123")
repo.create(artifact)

# 3. Save specific heavy parts
asr_data = ASRResults(transcription={"text": "Hello world"})
repo.save_part(artifact.id, "asr", asr_data)

# 4. Load only what's needed
loaded = repo.load(artifact.id, include=["asr"])
print(loaded.asr.transcription)  # Hello world
```

## Methods

- `create`: Initialize a new artifact (Root only).
- `save`: Batch save the artifact (Root + optional parts).
- `load`: Batch load the artifact (Root + optional parts).
- `save_part`: Atomically save/overwrite a specific part.
- `load_part`: Load a specific heavy part.
- `exists`: Check if a root artifact record exists.
- `delete`: Remove an artifact and its parts.

---
*For more examples, see [examples/repo.py](../../../examples/repo.py) (if available).*
