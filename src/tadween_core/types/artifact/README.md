# Artifact

The `artifact` package defines the core data model and its lazy-loaded parts in Tadween.

## Concepts

- **BaseArtifact**: An abstract contract for any artifact object (`id`, `status`, `metadata`).
- **TadweenArtifact**: The default artifact model with lazy-loaded parts (e.g., ASRResult, LLMResult).
- **ArtifactPart**: A base class for all heavy artifact parts. Inherit from this class to mark a field as a lazy-loaded part.

## Lazy Loading & Data Normalization

Large data volumes (e.g., audio transcriptions, speaker diarization) are split into:
- **Root**: Metadata, identity (aid), status.
- **Parts**: Large, lazy-loaded components.

This approach ensures minimal network overhead and memory usage when moving artifacts across different stages of a pipeline.

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

## Artifact Stages

`TadweenArtifact` includes a `current_stage` field to track its progress:
- `CREATED`: Initialized.
- `METADATA`: Metadata added.
- `ASR`: ASR results added.
- `NORMALIZED`: Results normalized.
- `LLM`: LLM analysis results added.
- `ERROR`: An error occurred.
- `DONE`: Processing completed.

---
*For more examples, see [examples/artifact.py](../../../examples/artifact.py) (if available).*
