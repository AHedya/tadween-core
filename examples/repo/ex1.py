from pathlib import Path

from my_artifact import (
    AudioArtifact,
    part_names,
    run_example,
)

from tadween_core.repo import SqliteRepo


def main():

    ROOT = Path(__file__).parent
    (ROOT / "temp").mkdir(exist_ok=True)

    # make yourself a favor by defining part_names as a literal and pass it
    # to save time remembering your fields
    repo = SqliteRepo[AudioArtifact, part_names](
        db_path=ROOT / "temp" / "temp.db",
        artifact_type=AudioArtifact,
    )

    run_example(repo)


if __name__ == "__main__":
    main()
