from pathlib import Path

import nox  # pyright: ignore[reportMissingImports]

nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True
PYTHON_VERSIONS = ["3.11", "3.12", "3.13", "3.14"]


@nox.session(python=PYTHON_VERSIONS, tags=["tests"])
def tests(session):
    session.run_install(
        "uv",
        "sync",
        "--group",
        "test",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("pytest", *session.posargs, env={"PYTHON_GIL": "1"})


@nox.session(python="3.11", tags=["style"])
def lint(session):
    session.run_install("uv", "pip", "install", "ruff")
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")


@nox.session(python=["3.14t"], tags=["ft_tests"])
def free_threaded_tests(session):
    session.run_install(
        "uv",
        "sync",
        "--group",
        "test",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("pytest", *session.posargs, env={"PYTHON_GIL": "0"})


@nox.session(python=PYTHON_VERSIONS, tags=["examples"])
def examples(session):
    session.run_install(
        "uv",
        "pip",
        "install",
        "-e",
        ".",
        "--group",
        "dev",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )

    example_path = Path("examples")
    example_files = [
        p
        for p in example_path.glob("**/*.py")
        if not p.name.startswith("_") and p.name != "__init__.py"
    ]

    for example in example_files:
        session.run("python", str(example), external=True)
