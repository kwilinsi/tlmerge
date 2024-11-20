from __future__ import annotations

from collections.abc import Callable
from enum import Enum
import logging
from queue import Queue, Empty
from threading import current_thread, Lock, Thread
from typing import Self

_log = logging.getLogger(__name__)


class WorkerPoolState(Enum):
    # Created the worker pool but haven't opened it in a context manager yet.
    # Can't add any tasks
    NOT_STARTED = 0

    # Accepting new tasks and actively completely them. This is the only state
    # where you can add tasks
    RUNNING = 1

    # No more tasks may be added. Waiting for all the workers to finish.
    CLOSED = 2

    # The pool reached the error threshold and is in the process of cancelling.
    # When adding new tasks, they are silently ignored
    CANCELLING = 3

    # Everything is done; all the tasks finished executing
    FINISHED = 4


class WorkerPoolExceptionGroup(ExceptionGroup):
    """
    This is the exception thrown when a WorkerPool exceeds its error threshold.
    It's an ExceptionGroup containing the exceptions thrown by the tasks.
    """

    @classmethod
    def from_errors(cls,
                    threshold: int,
                    errors: list[Exception]) -> WorkerPoolExceptionGroup:
        """
        Initialize a worker pool exception group.

        :param threshold: The max error threshold that was exceeded.
        :param errors: The list of errors that exceeded the threshold.
        """

        return cls(
            f"Too many errors: worker pool exceeded threshold of "
            f"{threshold} exception{'' if threshold == 1 else 's'}",
            errors
        )

    def derive(self, __excs):
        return WorkerPoolExceptionGroup(self.message, __excs)

    def summary(self) -> str:
        """
        Get a string with summary information.

        If there's only one exception in the group, this is the name of that
        exception class. If there's more than one exception, this is the string
        "[n] exceptions"

        :return: A string describing the contents of this exception group.
        """

        if len(self.exceptions) == 1:
            return self.exceptions[0].__class__.__name__
        else:
            return str(len(self.exceptions)) + ' exceptions'


class WorkerPool:
    def __init__(self,
                 max_workers: int = 1,
                 error_threshold: int = 0,
                 results: Queue = None,
                 name_prefix: str = 'wkr-',
                 on_close_hook: Callable | None = None,
                 daemon: bool = True) -> None:
        """
        Create a multithreaded worker pool. This is similar to a
        ThreadPoolExecutor, except that it allows (a) manual cancellation of
        the workers and (b) up to a certain threshold of tasks to raise errors
        before cancelling all remaining tasks.

        Note that KeyboardInterrupt, MemoryError, and SystemExit always
        cancel the remaining tasks, and are propagated to the caller regardless
        of the max_errors threshold.

        :param max_workers: The maximum number of concurrent worker threads.
        Defaults to 1.
        :param error_threshold: The maximum number of tasks that can raise
         exceptions before all remaining tasks are cancelled. Defaults to 0.
        :param results: An optional queue in which to put the result returned
         by each task. Defaults to None.
        :param name_prefix: The prefix to use for the names of the worker
         threads. This is followed with an incrementing integer starting from 1.
         Defaults to "wkr-".
        :param on_close_hook: A function that is called by each worker thread
         as it closes. Defaults to None.
        :param daemon: Whether each new worker should be created in daemon mode.
         Defaults to True.
        :raise ValueError: If the number of workers is zero or negative.
        """

        if max_workers <= 0:
            raise ValueError('Must have a positive max_workers count: '
                             f'got {max_workers}')

        self.on_close_hook: Callable | None = on_close_hook

        # Set the max error threshold and a list that's filled with the errors
        self._error_threshold = error_threshold
        self._errors: list[Exception] = []
        self._exception: BaseException | None = None

        # Keep track of the worker count
        self.max_workers: int = max_workers
        self._workers: list[Thread] = []

        # Counter that increases every a time new worker is created. This is
        # not the same as the number of current threads, len(self._workers)
        self._new_worker_counter: int = 0

        # Misc worker config
        self.name_prefix = name_prefix
        self.daemon = daemon

        # Queue with incoming tasks
        self._tasks: Queue[tuple[Callable[[...], any], str, tuple]] = Queue()

        # Results are added to this queue if it's given
        self._results: Queue | None = results

        # Keep track of the state (started, cancelling, etc.)
        self._state = WorkerPoolState.NOT_STARTED

        # This lock is used to enforce sequential read and write access to the
        # errors (self._errors and self._exception), state, (self._state),
        # and worker threads (self._workers and self._new_worker_counter)
        self._lock = Lock()

    def add(self,
            task: Callable[[...], any],
            identifier_text: str | None = None,
            *args) -> None:
        """
        Add a new task to run.

        :param task: The task to run.
        :param identifier_text: An optional string with which to identify the
         task in error messages if it fails.
        :param args: Arguments to pass to the task function.
        :return: None
        """

        if task is None:
            raise ValueError("Can't add task 'None' to worker pool")

        # Can only add a task while running
        with self._lock:
            if self._state == WorkerPoolState.NOT_STARTED:
                raise RuntimeError(
                    "Can't add a task to the worker pool before starting it. "
                    "You must open it in a context manager to start"
                )
            elif self._state == WorkerPoolState.CANCELLING:
                # Currently cancelled or in the process of cancelling.
                # Either way, silently ignore this. The error(s) will be raised
                # when exiting the context manager
                return
            elif self._state == WorkerPoolState.CLOSED:
                raise RuntimeError("Can't add a task to the worker pool "
                                   "after it's closed")
            elif self._state == WorkerPoolState.FINISHED:
                if self._exception is not None:
                    raise self._exception
                else:
                    raise RuntimeError("Can't add a task to the worker pool "
                                       "after it's finished")

            # Add the task to the queue
            self._tasks.put_nowait((task, identifier_text, args))

            # Create a new worker thread if under the maximum
            if len(self._workers) < self.max_workers:
                self._new_worker_counter += 1
                worker = Thread(
                    target=self._worker_loop,
                    name=self.name_prefix + str(self._new_worker_counter),
                    daemon=self.daemon
                )
                self._workers.append(worker)
                worker.start()

    def _worker_loop(self):
        """
        This function is run in each worker thread. It continously gets and
        runs the next task, until (a) there aren't any more tasks to run, or
        (b) this pool is no longer RUNNING.

        :return: None
        """

        try:
            while True:
                # If the pool is closed or cancelling, stop this worker
                with self._lock:
                    if self._state != WorkerPoolState.RUNNING:
                        return

                # Get the next task to run
                try:
                    task, identifier, args = self._tasks.get_nowait()
                except Empty:
                    # Stop this worker, as there's nothing left in the queue
                    return

                # Run it
                self._run_task(task, identifier, args)
        finally:
            # Run the close hook, if given
            if self.on_close_hook is not None:
                self.on_close_hook()

    def _run_task(self,
                  task: Callable[[...], any],
                  identifier: str | None,
                  args: tuple) -> None:
        """
        Run the given task from the queue.

        :param task: The task to run.
        :param identifier: An optional identifier of the task used in error
         messages.
        :param args: Arguments to pass to the task function.
        :return: None
        """

        try:
            # Run the task, adding to results queue if given
            if self._results is None:
                task(*args)
            else:
                self._results.put(task(*args))

            # Task finished successfully
            return
        except BaseException as e:
            # Catch all errors to them out below
            err = e

        # Log the error
        if identifier is None or not identifier.strip():
            identifier = 'Task'
        _log.error(f'{identifier} failed with {err.__class__.__name__}: {err}')

        with self._lock:
            fatal = isinstance(err, MemoryError) or \
                    not isinstance(err, Exception)

            if fatal:
                # Fatal exception. Set self._exception unless already set with
                # a fatal exception from an earlier thread
                if self._exception is None:
                    self._exception = err
            else:
                self._errors.append(err)  # noqa
                # If not yet reached the error threshold, exit
                if len(self._errors) <= self._error_threshold:
                    return

            # If already cancelling, exit this worker. Otherwise, set to
            # cancel, and wait for the other worker threads to finish
            if self._state == WorkerPoolState.CANCELLING:
                return
            else:
                self._state = WorkerPoolState.CANCELLING

        # Wait for all worker threads to finish by joining all except this one.
        # No need to lock as _workers is not modified once the state is no
        # longer RUNNING (and besides, locking blocks the other threads)
        for worker in self._workers:
            if worker is not current_thread():
                worker.join()

        # Shouldn't need the lock, as this is the only thread left, but eh,
        # just to be safe ig...
        with self._lock:
            # Set the exception, unless something already set a fatal error
            # (possibly even this worker a few lines up)
            if self._exception is None:
                self._exception = WorkerPoolExceptionGroup.from_errors(
                    self._error_threshold,
                    self._errors
                )

            # Change the state to FINISHED, as all worker threads are done
            self._state = WorkerPoolState.FINISHED

            # Log a warning that records the number of unfinished tasks
            t = self._tasks.qsize()
            if t > 0:
                _log.warning(f"{t} task{'' if t == 1 else 's'} "
                             "in worker pool not finished")

    def __enter__(self) -> Self:
        with self._lock:
            if self._state != WorkerPoolState.NOT_STARTED:
                raise RuntimeError(
                    f"Can't start worker pool in state {self._state.name}; "
                    f"expected {WorkerPoolState.NOT_STARTED.name}"
                )

            # Set the state to running; it now accepts tasks
            self._state = WorkerPoolState.RUNNING
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Cannot exit before it starts
        with self._lock:
            if self._state == WorkerPoolState.NOT_STARTED:
                raise RuntimeError("Can't stop worker pool before starting it")

            # If RUNNING, Mark CLOSED so new tasks aren't added
            if self._state == WorkerPoolState.RUNNING:
                self._state = WorkerPoolState.CLOSED

        # Wait for all workers to finish. No need to get the lock, as
        # self._workers isn't modified when not RUNNING
        for worker in self._workers:
            worker.join()

        try:
            # If there was already an exception while this context manager was
            # active, let it raise normally. (Returning true would suppress it)
            # https://docs.python.org/3.13/reference/datamodel.html#object.__exit__
            # This accounts for exceptions raised by calling add()
            if exc_val is not None:
                return

            # If there's an exception caused by enough workers failing (or just
            # one worker encountering a fatal error), raise it
            with self._lock:
                if self._exception is not None:
                    raise self._exception
        finally:
            # All workers finished
            self._state = WorkerPoolState.FINISHED
