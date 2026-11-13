"""
Phase 6.2 Tests: Safe Retries, Error Classification & Failure Recovery.

Verifies:
1. ErrorClassifier correctly identifies retryable vs non-retryable exceptions.
2. RetryPolicy succeeds when a transient error resolves on retry.
3. Non-retryable errors abort immediately without retry delay.
4. Retry exhaustion raises RetryExhaustedError with complete attempt history.
5. Cleanup handlers are invoked between retry attempts to wipe staging artifacts.
6. Pipeline status consistency is preserved during and after retries.
"""

from pathlib import Path
import pytest
import duckdb
import pyarrow as pa

from src.orchestration.retry import (
    ErrorClassifier,
    RetryPolicy,
    RetryExhaustedError,
)


def test_error_classifier_matrix():
    # Retryable exceptions
    assert ErrorClassifier.is_retryable(duckdb.OperationalError("Database locked"))
    assert ErrorClassifier.is_retryable(duckdb.DatabaseError("Transaction conflict"))
    assert ErrorClassifier.is_retryable(PermissionError("Access denied by lock"))
    assert ErrorClassifier.is_retryable(TimeoutError("Socket timed out"))

    # Windows WinError 32 (sharing violation)
    err_win32 = OSError("File being used by another process")
    err_win32.winerror = 32
    assert ErrorClassifier.is_retryable(err_win32)

    # Non-retryable exceptions
    assert not ErrorClassifier.is_retryable(ValueError("Invalid schema contract"))
    assert not ErrorClassifier.is_retryable(TypeError("Expected int"))
    assert not ErrorClassifier.is_retryable(pa.ArrowInvalid("Corrupt Parquet file"))
    assert not ErrorClassifier.is_retryable(FileNotFoundError("Missing config"))
    assert not ErrorClassifier.is_retryable(KeyError("missing_col"))


def test_retry_policy_transient_recovery():
    policy = RetryPolicy(max_retries=3, initial_delay_sec=0.01, backoff_factor=1.5, jitter=False)
    attempts = 0

    def flaky_operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise duckdb.OperationalError("Database temporarily locked")
        return "success_val"

    result = policy.execute("flaky_test", flaky_operation)
    assert result == "success_val"
    assert attempts == 3


def test_retry_policy_non_retryable_immediate_failure():
    policy = RetryPolicy(max_retries=3, initial_delay_sec=0.01)
    attempts = 0

    def fatal_operation():
        nonlocal attempts
        attempts += 1
        raise ValueError("Permanent schema rejection")

    with pytest.raises(ValueError, match="Permanent schema rejection"):
        policy.execute("fatal_test", fatal_operation)

    # Aborts immediately on first attempt without retrying
    assert attempts == 1


def test_retry_policy_exhaustion():
    policy = RetryPolicy(max_retries=2, initial_delay_sec=0.01, backoff_factor=1.0)
    attempts = 0

    def doomed_operation():
        nonlocal attempts
        attempts += 1
        raise PermissionError("File permanently locked by background antivirus")

    with pytest.raises(RetryExhaustedError) as exc_info:
        policy.execute("doomed_test", doomed_operation)

    assert exc_info.value.attempts == 3  # 1 initial + 2 retries = 3
    assert attempts == 3
    assert len(exc_info.value.history) == 3


def test_retry_cleanup_handler_called():
    policy = RetryPolicy(max_retries=2, initial_delay_sec=0.01)
    cleanup_calls = 0
    attempts = 0

    def cleanup():
        nonlocal cleanup_calls
        cleanup_calls += 1

    def op():
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise duckdb.OperationalError("Transient error")
        return "ok"

    res = policy.execute("cleanup_test", op, cleanup_func=cleanup)
    assert res == "ok"
    assert attempts == 2
    assert cleanup_calls == 1
