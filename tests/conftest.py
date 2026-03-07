"""Global pytest fixtures and configuration."""

from __future__ import annotations

from concurrent.futures import Future
import sys
from pathlib import Path
from typing import List

import pytest

# Ensure the src directory is on the Python path for src layout
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture
def fake_thread_pool(monkeypatch: pytest.MonkeyPatch) -> List[object]:
    """Patch ``ThreadPoolExecutor`` with a synchronous test double."""
    created_executors: List[object] = []

    class FakeThreadPoolExecutor:  # noqa: D401 - simple test double
        """Replacement executor that runs tasks immediately."""

        def __init__(self, max_workers: int | None = None) -> None:
            self.max_workers = max_workers
            self.submitted = []
            created_executors.append(self)

        def submit(self, fn, *args, **kwargs):  # noqa: ANN001 - matches executor API
            future: Future = Future()
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                future.set_exception(exc)
            else:
                future.set_result(result)
            self.submitted.append((fn, args, kwargs))
            return future

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr(
        "PermutiveAPI.utils.http.ThreadPoolExecutor", FakeThreadPoolExecutor
    )

    return created_executors
