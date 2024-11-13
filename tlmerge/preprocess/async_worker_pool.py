from __future__ import annotations

import asyncio
from asyncio import CancelledError, Task, Queue
from collections.abc import Coroutine
from enum import Enum
import logging
from typing import Self

_log = logging.getLogger(__name__)


class PoolState(Enum):
    # Created the worker pool but haven't opened it in an async context manager
    # yet. Can't add any tasks
    NOT_STARTED = 0

    # Accepting new tasks and actively completely them. This is the only state
    # where you can add tasks
    RUNNING = 1

    # No more tasks may be added. Waiting for all the workers to finish.
    CLOSED = 2

    # The pool reached the error threshold and is in the process of cancelling.
    # When adding new tasks, they are silently ignored
    CANCELLING = 3

    # The pool reached the error threshold and automatically stopped. This is
    # identical to CLOSED except that it indicates an error
    CANCELLED = 4

    # Everything is done; all the tasks finished executing
    FINISHED = 5


class AsyncPoolExceptionGroup(ExceptionGroup):
    """
    This is the exception thrown when an AsyncWorkerPool exceeds its error
    threshold. It's an ExceptionGroup containing the exceptions thrown by the
    tasks.
    """

    @classmethod
    def from_errors(cls,
                    threshold: int,
                    errors: list[Exception]) -> AsyncPoolExceptionGroup:
        """
        Initialize an async pool exception.
        :param threshold: The max error threshold that was exceeded.
        :param errors: The list of errors that exceeded the threshold.
        """

        return cls(
            f"Too many errors: worker pool exceeded threshold of "
            f"{threshold} exception{'' if threshold == 1 else 's'}",
            errors
        )

    def derive(self, __excs):
        return AsyncPoolExceptionGroup(self.message, __excs)


class AsyncWorkerPool:
    def __init__(self,
                 workers: int = 1,
                 max_errors: int = 0,
                 results: Queue | None = None) -> None:
        """
        Create an async worker pool. This is similar to a TaskGroup, except
        that it uses a fixed number of tasks to process a queue of t tasks. It
        also allows up to e tasks to raise errors before cancelling all
        remaining tasks.

        Within each task, asyncio.CancelledError exceptions are ignored. They
        stop the task but don't contribute to the max_errors threshold.
        KeyboardInterrupts immediately kill the worker pool and are propagated
        regardless of the max_errors.

        :param workers: The maximum number of concurrent tasks. Defaults to
        1, which effectively disables concurrency.
        :param max_errors: The maximum number of tasks that can raise exceptions
         before all remaining tasks are cancelled. Defaults to 0.
        :param results: An optional queue in which to put the result returned
         by each task. Defaults to None.
        :raises ValueError: If the number of workers is zero or negative.
        """

        if workers <= 0:
            raise ValueError('Must have a positive number of workers: '
                             f'got {workers}')

        # Set the max error threshold and a list that's filled with the errors
        self.error_threshold = max_errors
        self.errors: list[Exception] = []
        self.exception: BaseException | None = None

        # Queue with incoming tasks, and a counter for the number of tasks that
        # were started
        self.queue: Queue[Coroutine | None] = Queue()

        # Keep track of workers in order to cancel them (and in order to keep
        # their tasks alive; without a reference they may be garbage collected).
        self.workers: list[Task] = []
        self.max_workers: int = workers

        # Results are added to this queue if it's given
        self.results: Queue | None = results

        self.state: PoolState = PoolState.NOT_STARTED

        # Counter for unfinished/closed coroutines in the queue for debugging
        self.closed: int = 0

        _log.debug(f"Initialized async pool with {workers} "
                   f"worker{'' if workers == 1 else 's'} and {max_errors} "
                   f"allowed error{'' if max_errors == 1 else 's'}: "
                   f"{'includes' if results else 'no'} result queue")

    def add(self, coroutine: Coroutine) -> None:
        """
        Add a new coroutine to the task group.
        :param coroutine: The coroutine to add.
        :return: None
        :raises RuntimeError: If the pool isn't running.
        :raises ValueError: If the given coroutine is None.
        """

        if coroutine is None:
            raise ValueError("Can't add coroutine 'None' to async worker pool")
        if self.state == PoolState.NOT_STARTED:
            raise RuntimeError(
                "Can't add a task to the worker pool before starting it. "
                "Open in an async context manager to start"
            )
        elif self.state == PoolState.CANCELLING:
            # Currently in the process of cancelling. Silently ignore this
            return
        elif self.state == PoolState.CANCELLED:
            # Raise the exception explaining why it was cancelled
            raise self.exception
        elif self.state in (PoolState.CLOSED, PoolState.FINISHED):
            raise RuntimeError(
                "Can't add a task to the worker pool after it's "
                f"{self.state.name.lower()}."
            )

        # If we haven't reached the max number of workers yet, spawn a new one
        if len(self.workers) < self.max_workers:
            self.workers.append(asyncio.create_task(
                self._worker_loop(len(self.workers))
            ))

        # Add this task to the queue
        self.queue.put_nowait(coroutine)

    async def _worker_loop(self, worker_id: int):
        """
        Workers continuously query the queue for tasks to run and run them.

        The result of the coroutine is added to the results queue if enabled.

        :param worker_id: The index of this worker in the list of workers. This
         is used to avoid the worker cancelling itself.
        :return: None
        """

        # Continuously get and run the next task, until there's either an
        # exception or the next task is None
        while await self._run_next_task(worker_id):
            pass

    async def _run_next_task(self, worker_id: int) -> bool:
        """
        This is called by workers to get and run the next task from the queue.
        If there's an exception or the next task is None, this returns False,
        which signals that the worker should exit.

        :param worker_id: The id of the worker that called this.
        :return: True if and only if the next task was obtained and run
        successfully.
        """

        coroutine = None
        try:
            # Get the next coroutine to run.
            # If it's None, that's the signal to exit
            coroutine = await self.queue.get()
            if coroutine is None:
                return False

            # Run the coroutine. Add result to the results queue if given
            if self.results is None:
                await coroutine
            else:
                await self.results.put(await coroutine)

            return True
        except CancelledError:
            # This worker was likely cancelled by another one. Exit silently
            pass
        except (KeyboardInterrupt, MemoryError, SystemExit) as e:
            # These are fatal errors that immediately exceed the threshold

            if self.state.value < PoolState.CANCELLING.value:
                self.state = PoolState.CANCELLING

            self.exception = e

            # Cancel other workers
            for i, w in enumerate(self.workers):
                if i != worker_id:
                    w.cancel()
        except Exception as e:
            # Record and log this error
            self.errors.append(e)
            _log.error(e)

            # If the max error threshold is reached, cancel the pool
            if len(self.errors) == self.error_threshold + 1 and \
                    self.state.value < PoolState.CANCELLING.value:
                self.state = PoolState.CANCELLING

                # Cancel all the other workers
                for i, w in enumerate(self.workers):
                    if i != worker_id:
                        w.cancel()

                # Wait for each worker to finish
                await asyncio.gather(
                    *[w for i, w in enumerate(self.workers) if
                      i != worker_id],
                    return_exceptions=True
                )

                # Set the exception
                self.exception = AsyncPoolExceptionGroup.from_errors(
                    self.error_threshold,
                    self.errors
                )

                # Now completely cancelled
                self.state = PoolState.CANCELLED

        except BaseException as e:
            # All other base exceptions are immediately fatal
            if self.state.value < PoolState.CANCELLING.value:
                self.state = PoolState.CANCELLING
            self.exception = e
            for i, w in enumerate(self.workers):
                if i != worker_id:
                    w.cancel()

        # Got some exception. Close the coroutine (if obtained), and return
        # False to signal that the worker should exit
        if coroutine is not None:
            self.closed += 1
            coroutine.close()
        return False

    async def __aenter__(self) -> Self:
        if self.state != PoolState.NOT_STARTED:
            raise RuntimeError(
                "Can't start async worker pool in state "
                f"{self.state.name}; expected {PoolState.NOT_STARTED.name}"
            )

        # Set the state to running; it now accepts tasks
        self.state = PoolState.RUNNING
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # Can't exit if not started or finished
        if self.state in (PoolState.NOT_STARTED, PoolState.FINISHED):
            raise RuntimeError(
                f"Can't stop async worker pool in state {self.state.name}"
            )

        try:
            # If a pool exception was already raised (likely by calling add()),
            # do nothing. (Returning true would suppress it)
            # https://docs.python.org/3.13/reference/datamodel.html#object.__exit__
            if exc_val is not None and exc_val == self.exception:
                return

            # If RUNNING, Mark CLOSED so new tasks aren't added
            if self.state == PoolState.RUNNING:
                self.state = PoolState.CLOSED

            # Add None to the queue once for each worker, signaling them to stop
            for _ in range(len(self.workers)):
                self.queue.put_nowait(None)

            # Wait for all workers to finish
            await asyncio.gather(*self.workers, return_exceptions=True)

            # If the pool was cancelled, raise the exception explaining why,
            # and log a message with the number of closed tasks
            if self.state == PoolState.CANCELLED:
                # Close all remaining tasks
                try:
                    while self.queue.qsize() > 0:
                        task = self.queue.get_nowait()
                        if task is not None:
                            task.close()
                            self.closed += 1
                    _log.info(f"Closed {self.closed} unfinished "
                              f"task{'' if self.closed == 1 else 's'}")
                except Exception:
                    _log.error('Failed to close unfinished task(s)',
                               exc_info=True)
                finally:
                    raise self.exception
        finally:
            # Now completely finished
            self.state = PoolState.FINISHED
