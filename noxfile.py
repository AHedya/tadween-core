import nox  # pyright: ignore[reportMissingImports]

nox.options.default_venv_backend = "uv"


@nox.session(python=["3.11", "3.12", "3.13", "3.14"], tags=["tests"])
def tests(session):
    session.run_install(
        "uv",
        "sync",
        f"--python={session.virtualenv.location}",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    session.run("pytest")


@nox.session(python="3.11", tags=["style"])
def lint(session):
    session.run_install("uv", "pip", "install", "ruff")
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")
