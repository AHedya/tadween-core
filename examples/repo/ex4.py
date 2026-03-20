from pathlib import Path

from my_artifact import (
    AudioArtifact,
    part_names,
    run_example,
)

from tadween_core.repo.fs import FsRepo


def main():
    ROOT = Path(__file__).parent
    (ROOT / "temp").mkdir(exist_ok=True)

    repo = FsRepo[AudioArtifact, part_names](
        base_path=ROOT / "temp" / "json",
        artifact_type=AudioArtifact,
    )

    run_example(repo)


if __name__ == "__main__":
    main()
