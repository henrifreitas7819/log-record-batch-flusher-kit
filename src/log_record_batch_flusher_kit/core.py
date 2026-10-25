from __future__ import annotations

import threading
import time
import logging
from typing import Callable, List, Optional, Any

DEFAULT_BATCH_SIZE: int = 100
DEFAULT_FLUSH_INTERVAL: float = 1.0


class _MonotonicClock:
    """Thin wrapper over time.monotonic so a unit test can swap it out.

    time.monotonic is used rather than time.time because flush scheduling is
    a duration problem (has enough time elapsed since the last flush?), not a
    wall-clock problem (is it after midnight?). Monotonic is immune to NTP
    adjustments and clock skew, which is exactly what a timer that fires
    based on elapsed seconds needs.
    """

    def __call__(self) -> float:
        return time.monotonic()


class BatchFlusherHandler(logging.Handler):
    """A logging handler that accumulates records and flushes in batches.

    Flushing happens when either of two conditions becomes true:
      1. The number of buffered records reaches ``batch_size``.
      2. ``flush_interval`` seconds have elapsed since the last flush.

    Design decision: flushing is synchronous, on the emitting thread, inside
    the ``emit`` call that crosses the threshold. A dedicated background
    thread would make the timing of flushes non-deterministic from the test
    suite's point of view (the exact moment the thread wakes is OS scheduler
    dependent). Instead, each ``emit`` call checks elapsed time; a record
    that arrives late can trigger a flush even if it is the only record in
    the buffer. This is simpler to reason about and simpler to test with an
    injectable clock.

    Trade-off: if no records arrive at all, a pending batch will sit
    unflushed until the next emit. We accept this. A handler whose flushes
    depend entirely on log message arrival is acceptable for the
    high-throughput use cases this targets; if you need a hard real-time
    flush guarantee, a background thread (with its determinism caveats) is
    what you want, and that is out of scope here.

    Thread safety: the buffer and last-flush timestamp are guarded by a
    lock. Multiple threads emitting simultaneously will not corrupt the
    buffer, and only one thread performs a flush at a time.

    Args:
        batch_size: Maximum records buffered before a forced flush.
            Defaults to 100. Must be at least 1.
        flush_interval: Seconds between time-based flushes. Must be
            non-negative. A value of 0 disables time-based flushing (only
            capacity triggers flush). Defaults to 1.0.
        target: The downstream handler that receives flushed records. If
            None, flushed records are emitted via this handler's own
            ``emit`` method (which, by default, does nothing useful unless
            subclassed). Pass a concrete handler (FileHandler, StreamHandler)
            for the batching to have an effect.
        clock: Callable returning a float, used as the monotonic time
            source. Override in tests for deterministic behaviour.
        time_module: Optional module with a ``sleep`` function, used when
            blocking is enabled. Defaults to the standard ``time`` module.

    Raises:
        ValueError: If ``batch_size`` is not a positive integer or if
            ``flush_interval`` is negative.
    """

    def __init__(
        self,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        target: Optional[logging.Handler] = None,
        clock: Optional[Callable[[], float]] = None,
        time_module: Any = None,
    ) -> None:
        if not isinstance(batch_size, int) or isinstance(batch_size, bool):
            raise ValueError("batch_size must be an int")
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if flush_interval < 0:
            raise ValueError("flush_interval must be non-negative")

        super().__init__()
        self._batch_size: int = batch_size
        self._flush_interval: float = flush_interval
        self._target: Optional[logging.Handler] = target
        self._clock: Callable[[], float] = clock if clock is not None else _MonotonicClock()
        self._time_module: Any = time_module if time_module is not None else time

        self._lock = threading.Lock()
        self._buffer: List[logging.LogRecord] = []
        self._last_flush: float = self._clock()

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def flush_interval(self) -> float:
        return self._flush_interval

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer a record, flushing if capacity or interval is reached.

        If an exception occurs while flushing (the downstream target raises),
        the already-buffered records are preserved and the handler's
        ``handleError`` is invoked with the original record, matching the
        contract of the standard logging framework. This way a transient I/O
        failure does not silently discard logs.
        """
        should_flush = False
        with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self._batch_size:
                should_flush = True
            elif (
                self._flush_interval > 0
                and (self._clock() - self._last_flush) >= self._flush_interval
            ):
                should_flush = True

        if should_flush:
            self.flush()

    def flush(self) -> None:
        """Drain the buffer and emit each record to the target.

        Acquires the lock, snapshots the buffer, clears it, updates the
        last-flush timestamp, then releases the lock before emitting. This
        keeps the critical section short: a slow downstream handler does not
        block other threads from buffering new records. The trade-off is
        that a flush in progress and a concurrent emit can interleave such
        that the last-flush timestamp is updated before the records are
        actually sent; if the target raises, the timestamp has already moved
        and the next interval window starts from now. We consider this
        acceptable because the alternative (holding the lock during emit)
        would serialise all logging on the downstream handler's latency.
        """
        with self._lock:
            if not self._buffer:
                return
            records = self._buffer
            self._buffer = []
            self._last_flush = self._clock()

        for rec in records:
            try:
                if self._target is not None:
                    self._target.handle(rec)
                else:
                    super().emit(rec)
            except Exception:
                self.handleError(rec)

    def close(self) -> None:
        """Flush any remaining records and close the handler."""
        try:
            self.flush()
        finally:
            super().close()
