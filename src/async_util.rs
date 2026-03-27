//! Async utility primitives mirroring client-go `util/async`.
//!
//! This module groups the small execution building blocks used by the client's internal workers
//! and also exposed publicly for embedders that want the same callback / run-loop patterns:
//! [`Task`], [`Pool`], [`Executor`], [`Callback`], [`RunLoop`], and [`CancellationToken`].

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;

/// A boxed runnable task.
///
/// This mirrors client-go `util/async` where tasks are `func()`.
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// A pool can execute tasks asynchronously.
///
/// This mirrors client-go `util/async.Pool`.
pub trait Pool: Send + Sync + 'static {
    /// Submit a task to the pool.
    fn go(&self, task: Task);
}

/// An executor can append tasks for later execution.
///
/// This mirrors client-go `util/async.Executor`.
pub trait Executor: Pool {
    /// Append tasks to the executor. It must be safe to call concurrently.
    fn append(&self, tasks: Vec<Task>);
}

type InjectFunc<T, E> = Box<dyn FnOnce(T, Option<E>) -> (T, Option<E>) + Send + 'static>;
type CallbackFunc<T, E> = Box<dyn FnOnce(T, Option<E>) + Send + 'static>;

/// A callback that can be invoked immediately or scheduled to run later.
///
/// This mirrors client-go `util/async.Callback`.
pub struct Callback<T, E> {
    inner: Arc<CallbackInner<T, E>>,
}

impl<T, E> Clone for Callback<T, E> {
    fn clone(&self) -> Self {
        Callback {
            inner: self.inner.clone(),
        }
    }
}

impl<T, E> Callback<T, E> {
    /// Create a new callback associated with an executor.
    pub fn new(executor: Arc<dyn Executor>, f: impl FnOnce(T, Option<E>) + Send + 'static) -> Self {
        Callback {
            inner: Arc::new(CallbackInner {
                fulfilled: AtomicBool::new(false),
                executor,
                f: Mutex::new(Some(Box::new(f))),
                injected: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Return the executor used by this callback.
    pub fn executor(&self) -> Arc<dyn Executor> {
        self.inner.executor.clone()
    }

    /// Inject a deferred transformation to run before the callback.
    ///
    /// Injected functions are executed in reverse order of injection, matching client-go.
    pub fn inject(&self, g: impl FnOnce(T, Option<E>) -> (T, Option<E>) + Send + 'static) {
        let mut injected = self
            .inner
            .injected
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        injected.push(Box::new(g));
    }

    /// Invoke the callback immediately in the current thread.
    ///
    /// Only the first call among `invoke` and `schedule` will take effect.
    pub fn invoke(&self, val: T, err: Option<E>) {
        if !self.inner.fulfill() {
            return;
        }
        self.inner.call(val, err);
    }

    /// Schedule the callback to run later via its executor.
    ///
    /// Only the first call among `invoke` and `schedule` will take effect.
    pub fn schedule(&self, val: T, err: Option<E>)
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        if !self.inner.fulfill() {
            return;
        }

        let inner = self.inner.clone();
        self.inner
            .executor
            .append(vec![Box::new(move || inner.call(val, err))]);
    }
}

struct CallbackInner<T, E> {
    fulfilled: AtomicBool,
    executor: Arc<dyn Executor>,
    f: Mutex<Option<CallbackFunc<T, E>>>,
    injected: Mutex<Vec<InjectFunc<T, E>>>,
}

impl<T, E> CallbackInner<T, E> {
    fn fulfill(&self) -> bool {
        self.fulfilled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn call(&self, mut val: T, mut err: Option<E>) {
        let mut injected = {
            let mut injected = self.injected.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *injected)
        };

        while let Some(g) = injected.pop() {
            (val, err) = g(val, err);
        }

        let f = {
            let mut f = self.f.lock().unwrap_or_else(|e| e.into_inner());
            f.take()
        };
        if let Some(f) = f {
            f(val, err);
        }
    }
}

/// Execution state of a [`RunLoop`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// The run-loop is idle and not currently waiting or executing tasks.
    Idle,
    /// The run-loop is blocked waiting for new tasks to be appended.
    Waiting,
    /// The run-loop is currently draining queued tasks.
    Running,
}

/// Errors returned by [`RunLoop::exec`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RunLoopExecError {
    /// Another thread is already calling [`RunLoop::exec`].
    #[error("runloop: already executing")]
    AlreadyExecuting,
    /// Execution was cancelled after the given number of tasks had already run.
    #[error("runloop: cancelled")]
    Cancelled { executed: usize },
}

impl RunLoopExecError {
    /// Return how many tasks were executed before this error was reported.
    ///
    /// This is always `0` for [`RunLoopExecError::AlreadyExecuting`].
    pub fn executed(&self) -> usize {
        match self {
            RunLoopExecError::AlreadyExecuting => 0,
            RunLoopExecError::Cancelled { executed } => *executed,
        }
    }
}

/// A simple cancellation token used by [`RunLoop::exec`].
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Mark the token as cancelled.
    ///
    /// Future calls to [`CancellationToken::is_cancelled`] return `true`, and a
    /// currently running [`RunLoop::exec`] call will stop before executing any
    /// further queued tasks.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Return whether cancellation has been requested for this token.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// A single-threaded run-loop executor.
///
/// This mirrors client-go `util/async.RunLoop`. It is *not* an async runtime — tasks are plain
/// `FnOnce()` and are executed synchronously when calling [`RunLoop::exec`].
pub struct RunLoop {
    pool: Option<Arc<dyn Pool>>,
    ready: Condvar,
    inner: Mutex<RunLoopInner>,
}

struct RunLoopInner {
    runnable: Vec<Task>,
    state: State,
}

impl Default for RunLoop {
    fn default() -> Self {
        RunLoop::new()
    }
}

impl RunLoop {
    /// Create a new run-loop.
    pub fn new() -> Self {
        RunLoop {
            pool: None,
            ready: Condvar::new(),
            inner: Mutex::new(RunLoopInner {
                runnable: Vec::new(),
                state: State::Idle,
            }),
        }
    }

    /// Create a run-loop that uses a provided pool for [`RunLoop::go`].
    pub fn with_pool(pool: Arc<dyn Pool>) -> Self {
        RunLoop {
            pool: Some(pool),
            ..RunLoop::new()
        }
    }

    /// Set the pool used by [`RunLoop::go`].
    pub fn set_pool(&mut self, pool: Option<Arc<dyn Pool>>) {
        self.pool = pool;
    }

    /// Return the current state of the run-loop.
    pub fn state(&self) -> State {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).state
    }

    /// Return the number of runnable tasks currently queued.
    pub fn num_runnable(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .runnable
            .len()
    }

    /// Drive the run-loop until all queued tasks are executed.
    ///
    /// If `cancel` is triggered before completion, the method returns an error and any remaining
    /// unexecuted tasks stay queued.
    ///
    /// `exec` should only be called by a single thread at a time.
    pub fn exec(&self, cancel: &CancellationToken) -> Result<usize, RunLoopExecError> {
        let mut running = Vec::<Task>::new();
        let mut executed = 0usize;

        loop {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.state != State::Idle {
                return Err(RunLoopExecError::AlreadyExecuting);
            }

            if inner.runnable.is_empty() {
                inner.state = State::Waiting;
                while inner.state == State::Waiting
                    && inner.runnable.is_empty()
                    && !cancel.is_cancelled()
                {
                    let (guard, _) = self
                        .ready
                        .wait_timeout(inner, Duration::from_millis(5))
                        .unwrap_or_else(|e| e.into_inner());
                    inner = guard;
                }

                if cancel.is_cancelled() && inner.runnable.is_empty() {
                    inner.state = State::Idle;
                    return Err(RunLoopExecError::Cancelled { executed: 0 });
                }

                continue;
            }

            std::mem::swap(&mut running, &mut inner.runnable);
            inner.state = State::Running;
            drop(inner);
            break;
        }

        loop {
            {
                let mut drain = running.drain(..);
                loop {
                    if cancel.is_cancelled() {
                        let remaining: Vec<Task> = drain.collect();
                        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                        let mut combined = remaining;
                        combined.append(&mut inner.runnable);
                        inner.runnable = combined;
                        inner.state = State::Idle;
                        return Err(RunLoopExecError::Cancelled { executed });
                    }

                    match drain.next() {
                        Some(task) => {
                            task();
                            executed += 1;
                        }
                        None => break,
                    }
                }
            }

            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.runnable.is_empty() {
                inner.state = State::Idle;
                return Ok(executed);
            }

            std::mem::swap(&mut running, &mut inner.runnable);
            drop(inner);
        }
    }
}

impl Pool for RunLoop {
    fn go(&self, task: Task) {
        match &self.pool {
            Some(pool) => pool.go(task),
            None => {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn_blocking(task);
                } else {
                    std::thread::spawn(task);
                }
            }
        }
    }
}

impl Executor for RunLoop {
    fn append(&self, tasks: Vec<Task>) {
        if tasks.is_empty() {
            return;
        }

        let mut notify = false;
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.runnable.extend(tasks);
            if inner.state == State::Waiting {
                inner.state = State::Idle;
                notify = true;
            }
        }

        if notify {
            self.ready.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use std::time::Instant;

    use super::*;

    #[derive(Default)]
    struct MockExecutor {
        tasks: Mutex<Vec<Task>>,
    }

    impl MockExecutor {
        fn take_all(&self) -> Vec<Task> {
            let mut tasks = self.tasks.lock().unwrap();
            std::mem::take(&mut *tasks)
        }
    }

    impl Pool for MockExecutor {
        fn go(&self, task: Task) {
            self.append(vec![task]);
        }
    }

    impl Executor for MockExecutor {
        fn append(&self, tasks: Vec<Task>) {
            let mut queued = self.tasks.lock().unwrap();
            queued.extend(tasks);
        }
    }

    #[test]
    fn test_callback_inject_order() {
        let executor: Arc<dyn Executor> = Arc::new(MockExecutor::default());
        let cb = Callback::new(executor, |ns: Vec<i32>, err: Option<()>| {
            assert!(err.is_none());
            assert_eq!(vec![1, 2, 3], ns);
        });

        cb.inject(|mut ns, err| {
            ns.push(3);
            (ns, err)
        });
        cb.inject(|mut ns, err| {
            ns.push(2);
            (ns, err)
        });
        cb.inject(|mut ns, err| {
            ns.push(1);
            (ns, err)
        });
        cb.invoke(Vec::new(), None);
    }

    #[test]
    fn test_callback_fulfill_once_invoke_twice() {
        let executor: Arc<dyn Executor> = Arc::new(MockExecutor::default());
        let seen = Arc::new(Mutex::new(Vec::<i32>::new()));
        let seen_cb = seen.clone();
        let cb = Callback::new(executor, move |n, _err: Option<()>| {
            seen_cb.lock().unwrap().push(n);
        });

        cb.invoke(1, None);
        cb.invoke(2, None);
        assert_eq!(vec![1], *seen.lock().unwrap());
    }

    #[test]
    fn test_callback_fulfill_once_schedule_twice() {
        let executor = Arc::new(MockExecutor::default());
        let seen = Arc::new(Mutex::new(Vec::<i32>::new()));
        let seen_cb = seen.clone();
        let cb = Callback::new(executor.clone(), move |n, _err: Option<()>| {
            seen_cb.lock().unwrap().push(n);
        });

        cb.schedule(1, None);
        cb.schedule(2, None);
        let mut tasks = executor.take_all();
        assert_eq!(1, tasks.len());
        assert!(seen.lock().unwrap().is_empty());

        tasks.pop().unwrap()();
        assert_eq!(vec![1], *seen.lock().unwrap());
    }

    #[test]
    fn test_callback_fulfill_once_invoke_then_schedule() {
        let executor = Arc::new(MockExecutor::default());
        let seen = Arc::new(Mutex::new(Vec::<i32>::new()));
        let seen_cb = seen.clone();
        let cb = Callback::new(executor.clone(), move |n, _err: Option<()>| {
            seen_cb.lock().unwrap().push(n);
        });

        cb.invoke(1, None);
        cb.schedule(2, None);
        assert!(executor.take_all().is_empty());
        assert_eq!(vec![1], *seen.lock().unwrap());
    }

    #[test]
    fn test_callback_fulfill_once_schedule_then_invoke() {
        let executor = Arc::new(MockExecutor::default());
        let seen = Arc::new(Mutex::new(Vec::<i32>::new()));
        let seen_cb = seen.clone();
        let cb = Callback::new(executor.clone(), move |n, _err: Option<()>| {
            seen_cb.lock().unwrap().push(n);
        });

        cb.schedule(1, None);
        cb.invoke(2, None);
        let mut tasks = executor.take_all();
        assert_eq!(1, tasks.len());
        assert!(seen.lock().unwrap().is_empty());

        tasks.pop().unwrap()();
        assert_eq!(vec![1], *seen.lock().unwrap());
    }

    #[test]
    fn test_runloop_go() {
        let mut l = RunLoop::new();
        let n = Arc::new(AtomicU32::new(0));

        // go works without a pool
        let n_go = n.clone();
        l.go(Box::new(move || {
            n_go.store(1, Ordering::Release);
        }));
        let start = Instant::now();
        while n.load(Ordering::Acquire) != 1 {
            assert!(start.elapsed() < Duration::from_secs(1));
            std::thread::sleep(Duration::from_millis(1));
        }

        // use a custom pool
        let pool = Arc::new(MockExecutor::default());
        l.set_pool(Some(pool.clone()));
        let n_go = n.clone();
        l.go(Box::new(move || {
            n_go.store(2, Ordering::Release);
        }));
        assert_eq!(1, pool.tasks.lock().unwrap().len());
        assert_eq!(1, n.load(Ordering::Acquire));

        for task in pool.take_all() {
            task();
        }
        assert_eq!(2, n.load(Ordering::Acquire));
    }

    #[test]
    fn test_runloop_exec_wait() {
        let l = Arc::new(RunLoop::new());
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));
        let cancel = CancellationToken::default();

        let l_append = l.clone();
        let list_append = list.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            l_append.append(vec![Box::new(move || list_append.lock().unwrap().push(1))]);
        });

        let n = l.exec(&cancel).unwrap();
        assert_eq!(State::Idle, l.state());
        assert_eq!(0, l.num_runnable());
        assert_eq!(1, n);
        assert_eq!(vec![1], *list.lock().unwrap());
    }

    #[test]
    fn test_runloop_exec_once() {
        let l = Arc::new(RunLoop::new());
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));
        let cancel = CancellationToken::default();

        let l_task = l.clone();
        let list_task = list.clone();
        l.append(vec![Box::new(move || {
            let list_nested = list_task.clone();
            l_task.append(vec![Box::new(move || list_nested.lock().unwrap().push(2))]);
            list_task.lock().unwrap().push(1);
        })]);

        let n = l.exec(&cancel).unwrap();
        assert_eq!(State::Idle, l.state());
        assert_eq!(0, l.num_runnable());
        assert_eq!(2, n);
        assert_eq!(vec![1, 2], *list.lock().unwrap());
    }

    #[test]
    fn test_runloop_exec_twice() {
        let l = Arc::new(RunLoop::new());
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));
        let cancel = CancellationToken::default();

        let l_task = l.clone();
        let list_task = list.clone();
        l.append(vec![Box::new(move || {
            let l_delayed = l_task.clone();
            let list_delayed = list_task.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(1));
                l_delayed.append(vec![Box::new(move || list_delayed.lock().unwrap().push(2))]);
            });
            list_task.lock().unwrap().push(1);
        })]);

        let n = l.exec(&cancel).unwrap();
        assert_eq!(State::Idle, l.state());
        assert_eq!(0, l.num_runnable());
        assert_eq!(1, n);
        assert_eq!(vec![1], *list.lock().unwrap());

        let n = l.exec(&cancel).unwrap();
        assert_eq!(State::Idle, l.state());
        assert_eq!(0, l.num_runnable());
        assert_eq!(1, n);
        assert_eq!(vec![1, 2], *list.lock().unwrap());
    }

    #[test]
    fn test_runloop_exec_cancel_while_running() {
        let l = Arc::new(RunLoop::new());
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));
        let cancel = CancellationToken::default();

        let cancel_task = cancel.clone();
        let list_task = list.clone();
        let list_task2 = list.clone();
        l.append(vec![
            Box::new(move || {
                cancel_task.cancel();
                list_task.lock().unwrap().push(1);
            }),
            Box::new(move || list_task2.lock().unwrap().push(2)),
        ]);

        let err = l.exec(&cancel).unwrap_err();
        assert_eq!(RunLoopExecError::Cancelled { executed: 1 }, err);
        assert_eq!(State::Idle, l.state());
        assert_eq!(1, l.num_runnable());
        assert_eq!(vec![1], *list.lock().unwrap());
    }

    #[test]
    fn test_runloop_exec_cancel_while_waiting() {
        let l = Arc::new(RunLoop::new());
        let cancel = CancellationToken::default();

        let cancel_delayed = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            cancel_delayed.cancel();
        });

        let err = l.exec(&cancel).unwrap_err();
        assert_eq!(RunLoopExecError::Cancelled { executed: 0 }, err);
        assert_eq!(State::Idle, l.state());
        assert_eq!(0, l.num_runnable());
    }

    #[test]
    fn test_runloop_exec_concurrent() {
        let l = Arc::new(RunLoop::new());
        let cancel = CancellationToken::default();

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        l.append(vec![Box::new(move || {
            started_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        })]);

        let l_exec = l.clone();
        let cancel_exec = cancel.clone();
        let done = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let n = l_exec.exec(&cancel_exec).unwrap();
            assert_eq!(1, n);
            done.0.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let err = l.exec(&cancel).unwrap_err();
        assert_eq!(RunLoopExecError::AlreadyExecuting, err);
        done.1.recv_timeout(Duration::from_secs(1)).unwrap();
    }
}
