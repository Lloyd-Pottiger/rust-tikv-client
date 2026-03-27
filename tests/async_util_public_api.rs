use std::sync::{Arc, Mutex};

use tikv_client::async_util::{
    Callback, CancellationToken, Executor, Pool, RunLoop, RunLoopExecError, State, Task,
};

#[derive(Default)]
struct RecordingExecutor {
    tasks: Mutex<Vec<Task>>,
}

impl Pool for RecordingExecutor {
    fn go(&self, task: Task) {
        self.append(vec![task]);
    }
}

impl Executor for RecordingExecutor {
    fn append(&self, tasks: Vec<Task>) {
        self.tasks.lock().unwrap().extend(tasks);
    }
}

#[test]
fn async_util_public_api_exposes_callback_and_error_types() {
    let _: Option<Task> = None;
    let _: fn(Arc<dyn Executor>, fn(i32, Option<()>)) -> Callback<i32, ()> =
        Callback::<i32, ()>::new;
    let _: fn(&Callback<i32, ()>) -> Arc<dyn Executor> = Callback::<i32, ()>::executor;
    let _: fn(&Callback<i32, ()>, i32, Option<()>) = Callback::<i32, ()>::invoke;
    let _: fn(&Callback<i32, ()>, i32, Option<()>) = Callback::<i32, ()>::schedule;
    let _: fn(&RunLoop) -> State = RunLoop::state;
    let _: fn(&RunLoop) -> usize = RunLoop::num_runnable;
    let _: fn(&RunLoop, &CancellationToken) -> Result<usize, RunLoopExecError> = RunLoop::exec;
    let _: fn(&CancellationToken) = CancellationToken::cancel;
    let _: fn(&CancellationToken) -> bool = CancellationToken::is_cancelled;
    let _: fn(&RunLoopExecError) -> usize = RunLoopExecError::executed;

    let executor = Arc::new(RecordingExecutor::default());
    let executor_dyn: Arc<dyn Executor> = executor.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_callback = seen.clone();
    let callback = Callback::new(executor_dyn.clone(), move |value: i32, err: Option<()>| {
        assert!(err.is_none());
        seen_callback.lock().unwrap().push(value);
    });
    assert!(Arc::ptr_eq(&callback.executor(), &executor_dyn));

    callback.inject(|value, err| (value + 1, err));
    callback.schedule(6, None);
    executor.tasks.lock().unwrap().pop().unwrap()();
    assert_eq!(*seen.lock().unwrap(), vec![7]);

    let already = RunLoopExecError::AlreadyExecuting;
    assert_eq!(already.executed(), 0);
    let cancelled = RunLoopExecError::Cancelled { executed: 2 };
    assert_eq!(cancelled.executed(), 2);
}

#[test]
fn async_util_public_api_exposes_runloop_and_cancellation() {
    let mut run_loop = RunLoop::new();
    assert_eq!(run_loop.state(), State::Idle);
    assert_eq!(run_loop.num_runnable(), 0);

    let pool = Arc::new(RecordingExecutor::default());
    run_loop.set_pool(Some(pool.clone()));
    run_loop.go(Box::new(|| {}));
    assert_eq!(pool.tasks.lock().unwrap().len(), 1);

    let executed = Arc::new(Mutex::new(Vec::new()));
    let executed_task = executed.clone();
    run_loop.append(vec![Box::new(move || {
        executed_task.lock().unwrap().push(1)
    })]);
    assert_eq!(run_loop.num_runnable(), 1);

    let token = CancellationToken::default();
    assert!(!token.is_cancelled());
    assert_eq!(run_loop.exec(&token), Ok(1));
    assert_eq!(run_loop.state(), State::Idle);
    assert_eq!(*executed.lock().unwrap(), vec![1]);

    token.cancel();
    assert!(token.is_cancelled());
}
