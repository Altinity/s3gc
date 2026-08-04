import logging
import runpy
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def s3gc_module(monkeypatch):
    """Load the script with isolated command-line arguments and logging."""
    logger = logging.getLogger("s3gc_test")
    existing_handlers = list(logger.handlers)
    monkeypatch.setattr(sys, "argv", [str(ROOT / "s3gc.py")])
    module = runpy.run_path(str(ROOT / "s3gc.py"), run_name="s3gc_test")
    yield module

    for handler in list(logger.handlers):
        if handler not in existing_handlers:
            logger.removeHandler(handler)
            handler.close()
