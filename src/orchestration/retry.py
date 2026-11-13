"""
Safe Retries & Resilience Framework for SchemaGuard (Phase 6.2).

Provides bounded retries with exponential backoff, jitter, and error classification:
1. Differentiates retryable transient errors (lock contention, Windows file sharing violations)
   from permanent non-retryable failures (schema rejections, validation failures).
2. Avoids duplicate writes during retries via cleanup handlers and transaction rollback.
3. Preserves batch and run status consistency without producing false success states.
4. Bounded execution with deterministic retry exhaustion telemetry.
"""

import time
import random
import logging
from typing import Callable, Any, Optional, Tuple, Type, List
import duckdb
import pyarrow as pa

logger = logging.getLogger("schemaguard.retry")


class RetryExhaustedError(Exception):
    """Raised when an operation exhausts all configured retry attempts."""
    def __init__(self, operation_name: str, attempts: int, last_error: Exception, history: List[Exception]):
        self.operation_name = operation_name
        self.attempts = attempts
        self.last_error = last_error
        self.history = history
        super().__init__(
            f"Operation '{operation_name}' failed after {attempts} attempts. Last error: {last_error}"
        )


class ErrorClassifier:
    """
    Classifies execution exceptions into transient (retryable) and deterministic (non-retryable).
    """

    RETRYABLE_EXCEPTION_TYPES: Tuple[Type[Exception], ...] = (
        duckdb.OperationalError,
        duckdb.DatabaseError,
        getattr(duckdb, "TransactionException", duckdb.DatabaseError),
        getattr(duckdb, "IOException", OSError),
        PermissionError,
        BlockingIOError,
        TimeoutError,
        ConnectionError,
    )

    NON_RETRYABLE_EXCEPTION_TYPES: Tuple[Type[Exception], ...] = (
        pa.ArrowInvalid,
        pa.ArrowTypeError,
        ValueError,
        TypeError,
        KeyError,
        FileNotFoundError,
    )

    @classmethod
    def is_retryable(cls, exception: Exception) -> bool:
        """
        Determines if an exception is considered transient and safe for bounded retry.
        """
        if isinstance(exception, cls.NON_RETRYABLE_EXCEPTION_TYPES):
            return False

        if isinstance(exception, cls.RETRYABLE_EXCEPTION_TYPES):
            return True

        # Check for Windows sharing violation (WinError 32: file in use by another process)
        if isinstance(exception, OSError):
            winerror = getattr(exception, "winerror", None)
            if winerror in (32, 33):  # ERROR_SHARING_VIOLATION, ERROR_LOCK_VIOLATION
                return True
            # Generic transient I/O message checks
            msg = str(exception).lower()
            if "used by another process" in msg or "locked" in msg or "resource temporarily unavailable" in msg:
                return True

        return False


class RetryPolicy:
    """
    Bounded retry policy with exponential backoff and jitter.
    """

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay_sec: float = 0.05,
        backoff_factor: float = 2.0,
        max_delay_sec: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    ):
        self.max_retries = max(0, max_retries)
        self.initial_delay_sec = max(0.0, initial_delay_sec)
        self.backoff_factor = max(1.0, backoff_factor)
        self.max_delay_sec = max(self.initial_delay_sec, max_delay_sec)
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions

    def compute_delay(self, attempt: int) -> float:
        """Computes exponential backoff delay with optional random jitter."""
        delay = self.initial_delay_sec * (self.backoff_factor ** attempt)
        delay = min(delay, self.max_delay_sec)
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        return delay

    def execute(
        self,
        operation_name: str,
        func: Callable[..., Any],
        cleanup_func: Optional[Callable[[], None]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Executes func with bounded retries for retryable exceptions.
        Invokes cleanup_func (if provided) between attempts to clean staging files.
        """
        attempts = 0
        history: List[Exception] = []

        while True:
            attempts += 1
            try:
                return func(*args, **kwargs)
            except Exception as e:
                history.append(e)

                # Determine if retryable
                is_ret = False
                if self.retryable_exceptions:
                    is_ret = isinstance(e, self.retryable_exceptions)
                else:
                    is_ret = ErrorClassifier.is_retryable(e)

                if not is_ret:
                    # Non-retryable: re-raise immediately
                    raise

                if attempts > self.max_retries:
                    # Retry exhausted
                    raise RetryExhaustedError(
                        operation_name=operation_name,
                        attempts=attempts,
                        last_error=e,
                        history=history,
                    ) from e

                # Clean up staging state if handler provided
                if cleanup_func:
                    try:
                        cleanup_func()
                    except Exception as clean_err:
                        logger.warning(f"Cleanup handler failed during retry attempt {attempts}: {clean_err}")

                delay = self.compute_delay(attempts - 1)
                logger.warning(
                    f"Transient failure in '{operation_name}' (attempt {attempts}/{self.max_retries + 1}): {e}. "
                    f"Retrying in {delay:.3f}s..."
                )
                time.sleep(delay)
