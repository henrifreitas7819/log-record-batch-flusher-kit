import logging
import unittest

from log_record_batch_flusher_kit import (
    BatchFlusherHandler,
    DEFAULT_BATCH_SIZE,
    DEFAULT_FLUSH_INTERVAL,
)


class _FakeClock:
    """Manually-advanced monotonic clock for deterministic tests."""

    def __init__(self, start: float = 0.0):
        self._t = start

    def __call__(self):
        return self._t

    def advance(self, seconds: float):
        self._t += seconds


class _CollectHandler(logging.Handler):
    """Captures every record passed to handle() for assertions."""

    def __init__(self):
        super().__init__()
        self.records = []
        self.errors = []
        self.fail_next = False

    def emit(self, record):
        if self.fail_next:
            self.fail_next = False
            raise IOError("simulated downstream failure")
        self.records.append(record)


class TestDefaults(unittest.TestCase):
    def test_default_constants(self):
        self.assertEqual(DEFAULT_BATCH_SIZE, 100)
        self.assertEqual(DEFAULT_FLUSH_INTERVAL, 1.0)

    def test_defaults_applied(self):
        h = BatchFlusherHandler()
        self.assertEqual(h.batch_size, 100)
        self.assertEqual(h.flush_interval, 1.0)


class TestValidation(unittest.TestCase):
    def test_batch_size_must_be_positive(self):
        with self.assertRaises(ValueError):
            BatchFlusherHandler(batch_size=0)

    def test_batch_size_must_be_at_least_one(self):
        with self.assertRaises(ValueError):
            BatchFlusherHandler(batch_size=-5)

    def test_batch_size_must_be_int(self):
        with self.assertRaises(ValueError):
            BatchFlusherHandler(batch_size=1.5)

    def test_batch_size_rejects_bool(self):
        """bool is a subclass of int; a batch_size of True is not sensible."""
        with self.assertRaises(ValueError):
            BatchFlusherHandler(batch_size=True)

    def test_flush_interval_non_negative(self):
        with self.assertRaises(ValueError):
            BatchFlusherHandler(flush_interval=-0.1)

    def test_flush_interval_zero_allowed(self):
        h = BatchFlusherHandler(flush_interval=0)
        self.assertEqual(h.flush_interval, 0)


class TestCapacityFlush(unittest.TestCase):
    def setUp(self):
        self.target = _CollectHandler()
        self.clock = _FakeClock(0.0)
        self.handler = BatchFlusherHandler(
            batch_size=3,
            flush_interval=1000.0,
            target=self.target,
            clock=self.clock,
        )

    def _record(self, msg="hi"):
        return logging.LogRecord(
            "name", logging.INFO, "path", 1, msg, None, None
        )

    def test_no_flush_before_capacity(self):
        self.handler.emit(self._record())
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 0)

    def test_flush_at_capacity(self):
        self.handler.emit(self._record())
        self.handler.emit(self._record())
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 3)

    def test_buffer_cleared_after_flush(self):
        for _ in range(3):
            self.handler.emit(self._record())
        self.handler.flush()
        self.assertEqual(len(self.target.records), 3)

    def test_resume_after_flush(self):
        for _ in range(3):
            self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 3)
        self.handler.emit(self._record())
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 3)


class TestIntervalFlush(unittest.TestCase):
    def setUp(self):
        self.target = _CollectHandler()
        self.clock = _FakeClock(0.0)
        self.handler = BatchFlusherHandler(
            batch_size=1000,
            flush_interval=5.0,
            target=self.target,
            clock=self.clock,
        )

    def _record(self, msg="hi"):
        return logging.LogRecord(
            "name", logging.INFO, "path", 1, msg, None, None
        )

    def test_no_flush_before_interval(self):
        self.handler.emit(self._record())
        self.clock.advance(4.0)
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 0)

    def test_flush_after_interval(self):
        self.handler.emit(self._record())
        self.clock.advance(5.0)
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 2)

    def test_flush_exactly_at_interval(self):
        self.handler.emit(self._record())
        self.clock.advance(5.0)
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 2)

    def test_no_flush_when_interval_zero(self):
        h = BatchFlusherHandler(
            batch_size=1000,
            flush_interval=0,
            target=self.target,
            clock=self.clock,
        )
        h.emit(self._record())
        self.clock.advance(1000.0)
        h.emit(self._record())
        self.assertEqual(len(self.target.records), 0)

    def test_subsequent_interval_restart(self):
        self.handler.emit(self._record())
        self.clock.advance(5.0)
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 2)
        self.handler.emit(self._record())
        self.clock.advance(3.0)
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 2)
        self.clock.advance(2.0)
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 5)

    def test_single_record_can_trigger_interval_flush(self):
        self.handler.emit(self._record())
        self.clock.advance(5.0)
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 2)


class TestManualFlush(unittest.TestCase):
    def setUp(self):
        self.target = _CollectHandler()
        self.clock = _FakeClock(0.0)
        self.handler = BatchFlusherHandler(
            batch_size=1000,
            flush_interval=1000.0,
            target=self.target,
            clock=self.clock,
        )

    def _record(self, msg="hi"):
        return logging.LogRecord(
            "name", logging.INFO, "path", 1, msg, None, None
        )

    def test_manual_flush_empties_buffer(self):
        self.handler.emit(self._record())
        self.handler.emit(self._record())
        self.handler.flush()
        self.assertEqual(len(self.target.records), 2)

    def test_flush_empty_buffer_noop(self):
        self.handler.flush()
        self.assertEqual(len(self.target.records), 0)


class TestClose(unittest.TestCase):
    def setUp(self):
        self.target = _CollectHandler()
        self.clock = _FakeClock(0.0)
        self.handler = BatchFlusherHandler(
            batch_size=1000,
            flush_interval=1000.0,
            target=self.target,
            clock=self.clock,
        )

    def _record(self, msg="hi"):
        return logging.LogRecord(
            "name", logging.INFO, "path", 1, msg, None, None
        )

    def test_close_flushes_remaining(self):
        self.handler.emit(self._record())
        self.handler.emit(self._record())
        self.handler.close()
        self.assertEqual(len(self.target.records), 2)


class TestTargetError(unittest.TestCase):
    def setUp(self):
        self.target = _CollectHandler()
        self.clock = _FakeClock(0.0)
        self.handler = BatchFlusherHandler(
            batch_size=2,
            flush_interval=1000.0,
            target=self.target,
            clock=self.clock,
        )

    def _record(self, msg="hi"):
        return logging.LogRecord(
            "name", logging.INFO, "path", 1, msg, None, None
        )

    def test_downstream_error_calls_handleError(self):
        self.target.fail_next = True
        self.handler.emit(self._record())
        self.handler.emit(self._record())
        self.assertEqual(len(self.target.records), 1)


class TestIntegrationWithLogger(unittest.TestCase):
    def setUp(self):
        self.target = _CollectHandler()
        self.clock = _FakeClock(0.0)
        self.handler = BatchFlusherHandler(
            batch_size=2,
            flush_interval=1000.0,
            target=self.target,
            clock=self.clock,
        )
        self.logger = logging.getLogger("test_integration_batch_flusher")
        self.logger.handlers = [self.handler]
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

    def test_logger_emits_through_handler(self):
        self.logger.info("first")
        self.assertEqual(len(self.target.records), 0)
        self.logger.info("second")
        self.assertEqual(len(self.target.records), 2)
        self.assertEqual(self.target.records[0].getMessage(), "first")
        self.assertEqual(self.target.records[1].getMessage(), "second")


if __name__ == "__main__":
    unittest.main()
