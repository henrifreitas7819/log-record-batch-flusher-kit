# log-record-batch-flusher-kit

A `logging.Handler` that accumulates `LogRecord` instances and flushes them in batches to a downstream handler, reducing I/O calls for high-volume logging.

## Usage

```python
import logging
from log_record_batch_flusher_kit import BatchFlusherHandler

file_handler = logging.FileHandler("app.log")
batcher = BatchFlusherHandler(
    batch_size=500,
    flush_interval=2.0,
    target=file_handler,
)

logger = logging.getLogger("myapp")
logger.setLevel(logging.INFO)
logger.addHandler(batcher)
logger.propagate = False

logger.info("hello")
logger.info("world")
batcher.flush()
batcher.close()
```

Exported names: `BatchFlusherHandler`, `DEFAULT_BATCH_SIZE` (100), `DEFAULT_FLUSH_INTERVAL` (1.0).

## Why this exists

Every `logging.Handler.emit` call that writes to disk, sends to a socket, or hits a database incurs an I/O cost. Under high log volume, per-record writes dominate runtime. This handler buffers records in memory and flushes them in one pass when either a capacity threshold or a time threshold is crossed.

The trade-off: logs are not on disk the instant they are emitted. If the process crashes before a flush, buffered records are lost. Use this for high-throughput, loss-tolerant logging; do not use it for audit trails where every record must survive a crash.

## Design decisions

- **Synchronous flush on the emitting thread.** When a record crosses the capacity or interval threshold, the thread that emitted that record performs the flush. A background timer thread would make flush timing non-deterministic and hard to test without sleeping. The cost is that one thread bears the flush latency; the benefit is predictable, testable behaviour.
- **Injectable clock.** The constructor accepts a `clock` callable returning a float, used as the monotonic time source. Pass a fake in tests to advance time without sleeping.
- **`flush_interval=0` disables time-based flushing.** Only capacity triggers a flush. This is the escape hatch for callers who want pure size-based batching.
- **Thread safety.** The buffer and last-flush timestamp are guarded by a lock. The lock is released before records are emitted to the target, so a slow downstream does not block new records from being buffered.

## The awkward edge

If no records arrive after a batch is started, that batch sits unflushed indefinitely. There is no background thread to wake up and drain the buffer. If your application goes quiet for long stretches and you need pending logs persisted, call `flush()` at known points (e.g. on shutdown, or from your own periodic task). `close()` flushes any remaining records before closing.

## Run the tests

```
PYTHONPATH=src python -m unittest discover -s tests
```
