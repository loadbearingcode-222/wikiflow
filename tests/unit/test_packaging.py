"""Smoke tests for the project harness itself.

These exist so that Phase 0 has a real, passing gate before any pipeline code is
written: if these fail, the environment is wrong, not the logic.
"""

from __future__ import annotations

import sys

import pytest


@pytest.mark.unit
def test_package_imports() -> None:
    import wikiflow

    assert wikiflow.__version__


@pytest.mark.unit
def test_running_on_expected_python() -> None:
    """The container and the local venv must agree on the interpreter.

    The Airflow image is pinned to apache/airflow:3.3.1-python3.12, so a local venv on
    a different minor version could pass tests that fail in the container.
    """
    assert sys.version_info[:2] == (3, 12), f"expected Python 3.12, got {sys.version.split()[0]}"


@pytest.mark.unit
def test_wikiflow_does_not_import_airflow() -> None:
    """Guard the core architectural rule of this repo.

    src/wikiflow must stay orchestrator-agnostic. If someone adds `from airflow import ...`
    to a module here, the local test suite would stop working on Windows and the logic
    would become untestable without a scheduler. Fail loudly instead.
    """
    import importlib
    import pkgutil

    import wikiflow

    for module in pkgutil.walk_packages(wikiflow.__path__, prefix="wikiflow."):
        importlib.import_module(module.name)

    offenders = [name for name in sys.modules if name == "airflow" or name.startswith("airflow.")]
    assert not offenders, f"src/wikiflow must not import Airflow, but found: {offenders}"


@pytest.mark.unit
def test_runtime_imports_that_duckdb_makes_lazily_are_declared() -> None:
    """DuckDB imports pytz only when it returns a TIMESTAMPTZ value to Python.

    Nothing in the code imports it, so no linter sees the dependency. Locally it arrived
    through dbt; the dashboard image, which installs no dbt, failed to read the audit
    trail with `No module named 'pytz'` while every test here passed.
    """
    import tomllib
    from pathlib import Path

    pyproject = tomllib.loads(
        (Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8")
    )
    declared = {
        dep.split(">")[0].split("[")[0].strip() for dep in pyproject["project"]["dependencies"]
    }
    assert "pytz" in declared
