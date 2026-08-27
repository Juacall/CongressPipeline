"""
tests/test_main_prompt.py

Unit test verifying prompt validation behavior for table reset in scripts/main.py.
"""

from unittest.mock import patch
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from main import prompt_reset_tables


def test_prompt_reset_tables_accepts_yes():
    with patch("builtins.input", side_effect=["y"]):
        assert prompt_reset_tables() is True

    with patch("builtins.input", side_effect=["YES"]):
        assert prompt_reset_tables() is True


def test_prompt_reset_tables_accepts_no():
    with patch("builtins.input", side_effect=["n"]):
        assert prompt_reset_tables() is False

    with patch("builtins.input", side_effect=["NO"]):
        assert prompt_reset_tables() is False


def test_prompt_reset_tables_rejects_invalid_then_accepts_valid():
    """
    Verifies that invalid input (e.g. 'maybe', '123', 'foo') prompts again
    until a valid 'y' or 'n' is entered.
    """
    with patch("builtins.input", side_effect=["maybe", "foo", "123", "y"]):
        assert prompt_reset_tables() is True

    with patch("builtins.input", side_effect=["invalid", "n"]):
        assert prompt_reset_tables() is False


if __name__ == "__main__":
    test_prompt_reset_tables_accepts_yes()
    test_prompt_reset_tables_accepts_no()
    test_prompt_reset_tables_rejects_invalid_then_accepts_valid()
    print("All prompt validation tests passed successfully.")
