from __future__ import annotations

from collections.abc import Callable
from enum import Enum
import logging
from queue import Empty, Full, Queue
from threading import current_thread, Lock, Thread
from typing import Any, Self

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

    def derive(self, __excs) -> WorkerPoolExceptionGroup:
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
                 error_handler: Callable[[Exception, str], bool] | None = None,
                 task_queue_size: int = 0,
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
        :param error_handler: A function that is called whenever a worker
         counters an error. It must accept the Exception and the identifier
         string associated with the task. It can attempt to handle the error,
         returning anything Truthy if the error is handled (and thus shouldn't
         count toward the max error threshold). The handler is not used for
         fatal exceptions (i.e. MemoryError and other strictly BaseExceptions).
         If this is None, errors are simply logged. Defaults to None.
        :param task_queue_size: The maximum size of the task queue. If set,
         `add()` calls will block when the queue is full until a worker starts
         one of the tasks. If less than or equal to 0, the queue is unbounded.
         Defaults to 0.
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

        self._error_handler: Callable[[Exception, str], bool] | None = \
            error_handler

        # Misc worker config
        self.name_prefix = name_prefix
        self.daemon = daemon

        # Queue with incoming tasks
        self._tasks: Queue[tuple[Callable[..., Any], str, tuple]] = Queue(
            maxsize=task_queue_size
        )

        # Results are added to this queue if it's given
        self._results: Queue | None = results

        # Keep track of the state (started, cancelling, etc.)
        self._state = WorkerPoolState.NOT_STARTED

        # This lock is used to enforce sequential read and write access to the
        # errors (self._errors and self._exception), state, (self._state),
        # and worker threads (self._workers and self._new_worker_counter)
        self._lock = Lock()

    @property
    def state(self) -> WorkerPoolState:
        with self._lock:
            return self._state

    @property
    def error_count(self) -> int:
        """
        Get the number of errors so far.
        
        :return: The number of errors.
        """

        with self._lock:
            return len(self._errors)

    @property
    def worker_count(self) -> int:
        """
        Get the number of active worker threads.
        
        :return: The number of workers.
        """

        with self._lock:
            return len(self._workers)

    def add(self,
            task: Callable[..., Any],
            identifier_text: str | None = None,
            *args) -> None:
        """
        Add a new task to run.

        :param task: The task to run.
        :param identifier_text: An optional string with which to identify the
         task in error messages if it fails.
        :param args: Arguments to pass to the task function.
        :return: None
        :raises ValueError: If the given task is None.
        :raises RuntimeError: If the pool state is NOT_STARTED or CLOSED, or if
         it's FINISHED but was not cancelled due to one or more exceptions.
        :raises WorkerPoolExceptionGroup: If the pool was cancelled due to one
         or more tasks failing and exceeding the error threshold.
        :raises MemoryError: If the pool was cancelled due to any worker
         encountering a memory error.
        :raises BaseException: If the pool was cancelled due to a fatal
         BaseException. (Note: this is strictly an exception inheriting from
         BaseException but not Exception).
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
        i = 0
        while True:
            # Track the number of iterations. If this takes a while, log
            # warning messages
            if i == 5:
                _log.warning(f'Delayed {i} seconds while attempting to add '
                             f'task "{identifier_text}" to worker pool')
            elif i == 20 or i == 60:
                _log.warning(
                    'Worker pool running abnormally slow. Adding task '
                    f'"{identifier_text}" has stalled for {i} seconds'
                )
            elif i == 300:
                raise RuntimeError(
                    f'Request to add new task "{identifier_text}" to worker '
                    f'pool timed out after 5 minutes. The task queue is full.'
                )

            # Attempt to add this task
            try:
                self._tasks.put((task, identifier_text, args), timeout=1)
                break
            except Full:
                # Check whether the worker pool closed while waiting to
                # add this task to the queue
                with self._lock:
                    if self._state == WorkerPoolState.RUNNING:
                        # Still running; keep trying to add to queue
                        i += 1
                        continue
                    elif self._state == WorkerPoolState.FINISHED:
                        if self._exception is not None:
                            raise self._exception

                    raise RuntimeError(
                        f"Worker pool {self._state.name} while waiting "
                        "to add task, as task queue is full"
                    )

        # Create a new worker thread if under the maximum
        with self._lock:
            if len(self._workers) < self.max_workers:
                self._new_worker_counter += 1
                worker = Thread(
                    target=self._worker_loop,
                    name=self.name_prefix + str(self._new_worker_counter),
                    daemon=self.daemon
                )
                self._workers.append(worker)
                worker.start()

    def _worker_loop(self) -> None:
        """
        This function is run in each worker thread. It continuously gets and
        runs the next task, until (a) there aren't any more tasks to run, or
        (b) this pool is no longer RUNNING.

        :return: None
        """

        _log.debug('Started new worker')

        try:
            while True:
                # If the pool is cancelling, stop this worker
                with self._lock:
                    if self._state == WorkerPoolState.CANCELLING:
                        return

                # Get the next task to run
                try:
                    task, identifier, args = self._tasks.get_nowait()
                except Empty:
                    # Stop this worker, as there's nothing left in the queue
                    _log.debug('Task queue empty: removing this worker...')
                    return

                # Run it
                self._run_task(task, identifier, args)
        finally:
            try:
                # Remove this worker thread from the list of workers
                with self._lock:
                    self._workers.remove(current_thread())

                # Run the close hook, if given
                if self.on_close_hook is not None:
                    self.on_close_hook()
            finally:
                # If this is the last worker to end, switch state to FINISHED.
                # (Note: length of tasks queue isn't checked, as when cancelled
                # there may be unfinished tasks left over)
                with self._lock:
                    if self._state == WorkerPoolState.CLOSED and \
                            len(self._workers) == 0:
                        self._state = WorkerPoolState.FINISHED

                _log.debug('Finished removing this worker')

    def _run_task(self,
                  task: Callable[..., Any],
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

        # Get the task's identifier string
        if identifier is None or not identifier.strip():
            identifier = 'Task'

        with self._lock:
            fatal = isinstance(err, MemoryError) or \
                    not isinstance(err, Exception)

            if fatal or self._error_handler is None:
                _log.error(
                    f"{identifier} failed with{' fatal' if fatal else ''} "
                    f"{err.__class__.__name__}: {err}"
                )

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
            elif self._error_handler(err, identifier):  # noqa
                # If it's caught by the error handler, do nothing. (Noqa to
                # ignore warning that err is BaseException. It can't be, or
                # else fatal would be True, and this would be unreachable).
                return

            # If already cancelling, exit this worker. Otherwise, set to
            # cancel, and wait for the other worker threads to finish
            if self._state == WorkerPoolState.CANCELLING:
                return
            else:
                self._state = WorkerPoolState.CANCELLING

        # Wait for all worker threads to finish by repeatedly joining the
        # first one until they're all finished
        _log.debug('Cancelling: waiting for other workers to finish '
                   'before recording error(s)...')
        cur_thread = current_thread()
        i = 0
        while True:
            with self._lock:
                if len(self._workers) == 1:
                    break
                worker = self._workers[i]

            # If this thread is worker #1, don't join it (lest we deadlock).
            # Instead, start joining on worker #2.
            if worker == cur_thread:
                i = 1
                continue

            worker.join()

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

    def tasks(self) -> int:
        """
        Get the APPROXIMATE number of tasks currently enqueued.

        This uses `qsize()`, which is unreliable in that sense that the number
        of tasks may have changed by the time this method returns. See
        https://stackoverflow.com/q/1301416/10034073.

        :return: The approximate number of tasks.
        """

        return self._tasks.qsize()

    def current_workers(self) -> int:
        """
        Get the current number of worker threads.

        :return: The current number of workers.
        """

        with self._lock:
            return len(self._workers)

    def progress_str(self) -> str:
        """
        Get a string that summarizes the execution progress of this worker pool.
        This will be one of the following strings:

        - "not started"
        - "finished"
        - "running ([x] active workers and [y] enqueued tasks)"
        - "closed ([x] active workers and [y] enqueued tasks)"
        - "cancelling ([x] active workers and [y] enqueued tasks)"

        :return: A string summarizing what's going on with this worker pool.
        """

        with self._lock:
            if self._state == WorkerPoolState.NOT_STARTED:
                return "not started"
            elif self._state == WorkerPoolState.FINISHED:
                return "finished"
            else:
                w, q = len(self._workers), self._tasks.qsize()
                return (
                        self._state.name.lower() +
                        f" ({w} active worker{'' if w == 1 else 's'} and "
                        f"~{q} enqueued task{'' if q == 1 else 's'})"
                )

    def start(self) -> None:
        """
        Start this pool. It will now accept tasks via add().

        :return: None
        :raises RuntimeError: If the pool was already started.
        """

        _log.debug(f'Starting worker pool')

        with self._lock:
            if self._state != WorkerPoolState.NOT_STARTED:
                raise RuntimeError(
                    f"Can't start worker pool in state {self._state.name}; "
                    f"expected {WorkerPoolState.NOT_STARTED.name}"
                )

            # Set the state to running; it now accepts tasks
            self._state = WorkerPoolState.RUNNING

    def close(self, clear_tasks: bool = False) -> None:
        """
        Close this pool. It will no longer accept new tasks, but it will
        continue running until all existing tasks are finished.

        If the pool is already closed, finished, or cancelling, this has no
        effect. However, if it cancelled due to one or more exceptions, that
        exception (or exception group) is raised here.

        :param clear_tasks: Whether to remove all enqueued tasks (useful if
         closing due to a fatal error). Note that this does not affect
         workers currently processing a task. Defaults to False.
        :return: None
        :raises RuntimeError: If the pool state is NOT_STARTED.
        :raises WorkerPoolExceptionGroup: If the pool was cancelled due to one
         or more tasks failing and exceeding the error threshold.
        :raises MemoryError: If the pool was cancelled due to any worker
         encountering a memory error.
        :raises BaseException: If the pool was cancelled due to a fatal
         BaseException. (Note: this is strictly an exception inheriting from
         BaseException but not Exception).
        """

        _log.debug(f'Closing worker pool...')

        with self._lock:
            if self._state == WorkerPoolState.NOT_STARTED:
                # Can't close until started
                raise RuntimeError("Can't close worker pool before "
                                   "starting it")
            elif self._state == WorkerPoolState.RUNNING:
                # Only switch to CLOSED if currently RUNNING
                self._state = WorkerPoolState.CLOSED

            # If cancelled with an exception, raise the exception
            if self._exception is not None:
                raise self._exception
        
        # Clear remaining tasks if enabled
        if clear_tasks:
            _log.debug(f'Clearing remaining tasks (~{self._tasks.qsize()})...')
            with self._tasks.mutex:
                self._tasks.queue.clear()

        _log.debug(f'Successfully closed worker pool')

    def join(self, diagnostics: bool = False) -> None:
        """
        Block the calling thread until all the workers finish. If they finish
        due to one or more exceptions, they are raised.

        You cannot join the pool until after closing it to reject new tasks.
        
        :param diagnostics: Whether to log some diagnostic info about the
         number of remaining tasks. Defaults to False.
        :return: None
        """

        _log.debug(f'Joining worker pool (blocking thread)...')

        if diagnostics:
            _log.info(f'Waiting on worker pool: {self.progress_str()}')

        with self._lock:
            if self._state in (WorkerPoolState.NOT_STARTED,
                               WorkerPoolState.RUNNING):
                raise RuntimeError("Can't join worker pool while in "
                                   f"{self._state.name} state")
            elif self._state == WorkerPoolState.FINISHED:
                # Raise exception if there is one; otherwise exit
                if self._exception is not None:
                    raise self._exception
                return

        # Block while waiting for all workers to finish
        while True:
            with self._lock:
                if len(self._workers) == 0:
                    break
                worker = self._workers[0]
            worker.join()

        # If cancelled, raise the exception(s)
        if self._exception is not None:
            raise self._exception

        _log.debug(f'Finished blocking while joining on worker pool')

    def is_finished(self) -> bool:
        """
        Check whether this pool is finished.

        :return: True if and only if it has completely finished.
        """

        with self._lock:
            return self._state == WorkerPoolState.FINISHED

    def __enter__(self) -> Self:
        _log.debug(f'Entering worker pool context manager...')

        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the context manager. This closes the worker pool (so it no longer
        accepts tasks) and then waits for all workers to finish.

        :param exc_type: The type of exception raised within the context
         manager, if any.
        :param exc_val: The exception raised within the context manager, if any.
        :param exc_tb: The traceback of an exception raised within the context
         manager, if any.
        :return: None
        :raises WorkerPoolExceptionGroup: If the pool was cancelled due to
         tasks exceeding the max error threshold.
        :raises BaseException: If the pool was cancelled due to one or more
         workers encountering a fatal error (i.e. a MemoryError or any
         BaseException that is not an Exception).
        """

        _log.debug(f'Exiting worker pool context manager. Pool will close')

        self.close()

        try:
            self.join()
        except BaseException as e:
            # If there was already an exception while this context manager was
            # active, add it to this one
            if exc_val is not None:
                raise e from exc_val
            raise e

        _log.debug(f'Exited worker pool context manager')
