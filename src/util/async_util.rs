// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go client parity: `client-go/util/async/*`.

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use thiserror::Error;
use tokio::sync::Notify;

pub(crate) trait Executor: Send + Sync + 'static {
    fn go(&self, f: Box<dyn FnOnce() + Send + 'static>);
}

type Injection<T, E> = Arc<dyn Fn(T, E) -> (T, E) + Send + Sync + 'static>;
type CallbackFn<T, E> = Box<dyn FnOnce(T, E) + Send + 'static>;

/// A single-shot callback with optional injectors.
///
/// Mirrors `client-go/util/async/callback.go` semantics used by core_test.go:
/// - `inject` is LIFO (executed in reverse order)
/// - `invoke` and `schedule` fulfill at most once (first call wins)
#[derive(Clone)]
pub(crate) struct Callback<T, E> {
    inner: Arc<CallbackInner<T, E>>,
}

struct CallbackInner<T, E> {
    executor: Arc<dyn Executor>,
    fulfilled: AtomicBool,
    injections: Mutex<Vec<Injection<T, E>>>,
    callback: Mutex<Option<CallbackFn<T, E>>>,
}

impl<T: Send + 'static, E: Send + 'static> Callback<T, E> {
    pub(crate) fn new(executor: Arc<dyn Executor>, f: impl FnOnce(T, E) + Send + 'static) -> Self {
        Self {
            inner: Arc::new(CallbackInner {
                executor,
                fulfilled: AtomicBool::new(false),
                injections: Mutex::new(Vec::new()),
                callback: Mutex::new(Some(Box::new(f))),
            }),
        }
    }

    pub(crate) fn inject(&self, f: impl Fn(T, E) -> (T, E) + Send + Sync + 'static) {
        self.inner.injections.lock().unwrap().push(Arc::new(f));
    }

    pub(crate) fn invoke(&self, v: T, e: E) {
        if self.inner.fulfilled.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.run(v, e);
    }

    pub(crate) fn schedule(&self, v: T, e: E) {
        if self.inner.fulfilled.swap(true, Ordering::AcqRel) {
            return;
        }

        let inner = self.inner.clone();
        self.inner.executor.go(Box::new(move || inner.run(v, e)));
    }
}

impl<T: Send + 'static, E: Send + 'static> CallbackInner<T, E> {
    fn run(&self, mut v: T, mut e: E) {
        let injections = self.inner_injections_snapshot();
        for inj in injections.into_iter().rev() {
            (v, e) = inj(v, e);
        }

        if let Some(cb) = self.callback.lock().unwrap().take() {
            cb(v, e);
        }
    }

    fn inner_injections_snapshot(&self) -> Vec<Injection<T, E>> {
        self.injections.lock().unwrap().clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunLoopState {
    Idle,
    Running,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum RunLoopError {
    #[error("runloop exec is already running")]
    ConcurrentExec { executed: usize },
    #[error("runloop exec canceled")]
    Canceled { executed: usize },
}

#[derive(Debug, Clone)]
pub(crate) struct ExecContext {
    inner: Arc<ExecContextInner>,
}

#[derive(Debug)]
struct ExecContextInner {
    canceled: AtomicBool,
    notify: Notify,
}

impl ExecContext {
    pub(crate) fn background() -> Self {
        Self {
            inner: Arc::new(ExecContextInner {
                canceled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn with_cancel() -> (Self, ExecCancel) {
        let ctx = Self::background();
        let cancel = ExecCancel {
            inner: ctx.inner.clone(),
        };
        (ctx, cancel)
    }

    fn is_canceled(&self) -> bool {
        self.inner.canceled.load(Ordering::Acquire)
    }

    async fn canceled(&self) {
        if self.is_canceled() {
            return;
        }
        self.inner.notify.notified().await;
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExecCancel {
    inner: Arc<ExecContextInner>,
}

impl ExecCancel {
    pub(crate) fn cancel(&self) {
        if !self.inner.canceled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
        }
    }
}

/// A small serial executor for `FnOnce()` tasks, with optional waiting.
///
/// Mirrors `client-go/util/async/runloop.go` semantics used by runloop_test.go.
#[derive(Clone)]
pub(crate) struct RunLoop {
    inner: Arc<RunLoopInner>,
}

struct RunLoopInner {
    pool: Mutex<Option<Arc<dyn Executor>>>,
    state: Mutex<RunLoopState>,
    queue: Mutex<VecDeque<Box<dyn FnOnce() + Send + 'static>>>,
    notify: Notify,
    exec_running: AtomicBool,
}

impl RunLoop {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RunLoopInner {
                pool: Mutex::new(None),
                state: Mutex::new(RunLoopState::Idle),
                queue: Mutex::new(VecDeque::new()),
                notify: Notify::new(),
                exec_running: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn pool(&self) -> Option<Arc<dyn Executor>> {
        self.inner.pool.lock().unwrap().clone()
    }

    pub(crate) fn set_pool(&self, pool: Option<Arc<dyn Executor>>) {
        *self.inner.pool.lock().unwrap() = pool;
    }

    pub(crate) fn state(&self) -> RunLoopState {
        *self.inner.state.lock().unwrap()
    }

    pub(crate) fn num_runnable(&self) -> usize {
        self.inner.queue.lock().unwrap().len()
    }

    pub(crate) fn go(&self, f: impl FnOnce() + Send + 'static) {
        if let Some(pool) = self.pool() {
            pool.go(Box::new(f));
            return;
        }

        tokio::spawn(async move { f() });
    }

    pub(crate) fn append(&self, f: impl FnOnce() + Send + 'static) {
        self.inner.queue.lock().unwrap().push_back(Box::new(f));
        self.inner.notify.notify_one();
    }

    pub(crate) async fn exec(&self, ctx: &ExecContext) -> Result<usize, RunLoopError> {
        if self
            .inner
            .exec_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RunLoopError::ConcurrentExec { executed: 0 });
        }
        struct Guard<'a> {
            inner: &'a RunLoopInner,
        }
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                *self.inner.state.lock().unwrap() = RunLoopState::Idle;
                self.inner.exec_running.store(false, Ordering::Release);
            }
        }
        let _guard = Guard { inner: &self.inner };

        *self.inner.state.lock().unwrap() = RunLoopState::Running;

        let mut n = 0usize;
        loop {
            if ctx.is_canceled() {
                return Err(RunLoopError::Canceled { executed: n });
            }

            let task = self.inner.queue.lock().unwrap().pop_front();
            if let Some(task) = task {
                task();
                n += 1;
                continue;
            }

            if n > 0 {
                return Ok(n);
            }

            tokio::select! {
                _ = self.inner.notify.notified() => {}
                _ = ctx.canceled() => return Err(RunLoopError::Canceled { executed: n }),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[derive(Default)]
    struct MockExecutor {
        tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    }

    impl MockExecutor {
        fn push(&self, f: Box<dyn FnOnce() + Send + 'static>) {
            self.tasks.lock().unwrap().push(f);
        }

        fn pop_all(&self) -> Vec<Box<dyn FnOnce() + Send + 'static>> {
            std::mem::take(&mut *self.tasks.lock().unwrap())
        }

        fn len(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }
    }

    impl Executor for MockExecutor {
        fn go(&self, f: Box<dyn FnOnce() + Send + 'static>) {
            self.push(f);
        }
    }

    #[test]
    fn test_inject_order() {
        let e = Arc::new(MockExecutor::default());
        let cb = Callback::new(e, |mut ns: Vec<i32>, err: Option<String>| {
            assert!(err.is_none());
            assert_eq!(ns, vec![1, 2, 3]);
            ns.push(4);
            drop(ns);
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

        cb.invoke(Vec::<i32>::new(), None);
    }

    #[test]
    fn test_fulfill_once_invoke_twice() {
        let e = Arc::new(MockExecutor::default());
        let out = Arc::new(Mutex::new(Vec::<i32>::new()));
        let out2 = out.clone();
        let cb = Callback::new(e, move |n: i32, _err: Option<String>| {
            out2.lock().unwrap().push(n)
        });

        cb.invoke(1, None);
        cb.invoke(2, None);
        assert_eq!(*out.lock().unwrap(), vec![1]);
    }

    #[test]
    fn test_fulfill_once_schedule_twice() {
        let e = Arc::new(MockExecutor::default());
        let out = Arc::new(Mutex::new(Vec::<i32>::new()));
        let out2 = out.clone();
        let cb = Callback::new(e.clone(), move |n: i32, _err: Option<String>| {
            out2.lock().unwrap().push(n)
        });

        cb.schedule(1, None);
        cb.schedule(2, None);

        assert_eq!(e.len(), 1);
        assert!(out.lock().unwrap().is_empty());

        for task in e.pop_all() {
            task();
        }
        assert_eq!(*out.lock().unwrap(), vec![1]);
    }

    #[test]
    fn test_fulfill_once_invoke_then_schedule() {
        let e = Arc::new(MockExecutor::default());
        let out = Arc::new(Mutex::new(Vec::<i32>::new()));
        let out2 = out.clone();
        let cb = Callback::new(e.clone(), move |n: i32, _err: Option<String>| {
            out2.lock().unwrap().push(n)
        });

        cb.invoke(1, None);
        cb.schedule(2, None);

        assert_eq!(e.len(), 0);
        assert_eq!(*out.lock().unwrap(), vec![1]);
    }

    #[test]
    fn test_fulfill_once_schedule_then_invoke() {
        let e = Arc::new(MockExecutor::default());
        let out = Arc::new(Mutex::new(Vec::<i32>::new()));
        let out2 = out.clone();
        let cb = Callback::new(e.clone(), move |n: i32, _err: Option<String>| {
            out2.lock().unwrap().push(n)
        });

        cb.schedule(1, None);
        cb.invoke(2, None);

        assert_eq!(e.len(), 1);
        assert!(out.lock().unwrap().is_empty());

        for task in e.pop_all() {
            task();
        }
        assert_eq!(*out.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn test_go() {
        let l = RunLoop::new();
        let n = Arc::new(AtomicU32::new(0));

        assert!(l.pool().is_none());
        let n1 = n.clone();
        l.go(move || n1.store(1, Ordering::Relaxed));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            if n.load(Ordering::Relaxed) == 1 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timeout waiting for Go()");
            }
            sleep(Duration::from_millis(1)).await;
        }

        let pool = Arc::new(MockExecutor::default());
        l.set_pool(Some(pool.clone()));

        let n2 = n.clone();
        l.go(move || n2.store(2, Ordering::Relaxed));

        assert_eq!(pool.len(), 1);
        assert_eq!(n.load(Ordering::Relaxed), 1);

        for task in pool.pop_all() {
            task();
        }
        assert_eq!(n.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_exec_wait() {
        let l = RunLoop::new();
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));

        let l2 = l.clone();
        let list2 = list.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(1)).await;
            l2.append(move || list2.lock().unwrap().push(1));
        });

        let n = l.exec(&ExecContext::background()).await.unwrap();
        assert_eq!(l.state(), RunLoopState::Idle);
        assert_eq!(l.num_runnable(), 0);
        assert_eq!(n, 1);
        assert_eq!(*list.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn test_exec_once_runs_appended_tasks() {
        let l = RunLoop::new();
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));

        let l2 = l.clone();
        let list2 = list.clone();
        l.append(move || {
            let l3 = l2.clone();
            let list3 = list2.clone();
            l3.append(move || list3.lock().unwrap().push(2));
            list2.lock().unwrap().push(1);
        });

        let n = l.exec(&ExecContext::background()).await.unwrap();
        assert_eq!(l.state(), RunLoopState::Idle);
        assert_eq!(l.num_runnable(), 0);
        assert_eq!(n, 2);
        assert_eq!(*list.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_exec_twice() {
        let l = RunLoop::new();
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));

        let l2 = l.clone();
        let list2 = list.clone();
        l.append(move || {
            let l3 = l2.clone();
            let list3 = list2.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(1)).await;
                l3.append(move || list3.lock().unwrap().push(2));
            });
            list2.lock().unwrap().push(1);
        });

        let n = l.exec(&ExecContext::background()).await.unwrap();
        assert_eq!(l.state(), RunLoopState::Idle);
        assert_eq!(l.num_runnable(), 0);
        assert_eq!(n, 1);
        assert_eq!(*list.lock().unwrap(), vec![1]);

        let n = l.exec(&ExecContext::background()).await.unwrap();
        assert_eq!(l.state(), RunLoopState::Idle);
        assert_eq!(l.num_runnable(), 0);
        assert_eq!(n, 1);
        assert_eq!(*list.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_exec_cancel_while_running() {
        let (ctx, cancel) = ExecContext::with_cancel();

        let l = RunLoop::new();
        let list = Arc::new(Mutex::new(Vec::<i32>::new()));
        let list2 = list.clone();
        let cancel2 = cancel.clone();
        l.append(move || {
            cancel2.cancel();
            list2.lock().unwrap().push(1);
        });
        let list3 = list.clone();
        l.append(move || list3.lock().unwrap().push(2));

        let err = l.exec(&ctx).await.unwrap_err();
        assert_eq!(err, RunLoopError::Canceled { executed: 1 });
        assert_eq!(l.state(), RunLoopState::Idle);
        assert_eq!(l.num_runnable(), 1);
        assert_eq!(*list.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn test_exec_cancel_while_waiting() {
        let (ctx, cancel) = ExecContext::with_cancel();
        let l = RunLoop::new();
        tokio::spawn(async move {
            sleep(Duration::from_millis(1)).await;
            cancel.cancel();
        });

        let err = l.exec(&ctx).await.unwrap_err();
        assert_eq!(err, RunLoopError::Canceled { executed: 0 });
        assert_eq!(l.state(), RunLoopState::Idle);
        assert_eq!(l.num_runnable(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_exec_concurrent() {
        let l = RunLoop::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        l.append(move || {
            let _ = started_tx.send(());
            std::thread::sleep(Duration::from_millis(50));
        });

        let l2 = l.clone();
        let done = tokio::spawn(async move { l2.exec(&ExecContext::background()).await });

        started_rx
            .await
            .expect("expected first exec to start running");
        let res = l.exec(&ExecContext::background()).await;
        assert_eq!(
            res.unwrap_err(),
            RunLoopError::ConcurrentExec { executed: 0 }
        );

        let n = done.await.unwrap().unwrap();
        assert_eq!(n, 1);
    }
}
