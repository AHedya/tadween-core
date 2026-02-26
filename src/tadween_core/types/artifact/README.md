# Artifact

The `artifact` package defines the core data model and its lazy-loaded parts.

## Concepts

- **BaseArtifact**: An abstract contract for any artifact object.
- **ArtifactPart**: A base class for all heavy artifact parts. Inherit from this class to mark a field as a lazy-loaded part.
- **TadweenArtifact**: The default artifact model. Battery-included.

## Anatomy

```
└── artifact
    ├── __init__.py
    ├── base.py     => Defines `BaseArtifact` and `ArtifactPart` contracts.
    ├── README.md
    └── tadween.py  => Defines `TadweenArtifact` and implements helpers.
```
## Lazy Loading & Data Normalization

Large data volumes (e.g., audio transcriptions, speaker diarization) are split into:
- **Root**: Metadata, identity (aid), status.
- **Parts**: Large, lazy-loaded components.

This approach ensures minimal network overhead and memory usage when moving artifacts across different stages of a pipeline. In addition to resume-ability

## Usage Example

```python
from tadween_core.types.artifact import TadweenArtifact, ASRResults

# 1. Create an artifact
artifact = TadweenArtifact(id="artifact-123")

# 2. Add heavy parts
asr_data = ASRResults(transcription={"text": "Hello world"})
artifact.asr = asr_data

# 3. Access parts
print(artifact.asr.transcription)  # Hello world
```

