use std::sync::{Arc, Mutex};

use tikv_client::util::r#async::{Callback, Executor, Pool, RunLoop, Task};

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
fn util_async_public_api_exposes_runloop_and_callback() {
    let run_loop = RunLoop::new();
    assert_eq!(run_loop.num_runnable(), 0);

    let executor = Arc::new(RecordingExecutor::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_callback = seen.clone();
    let callback = Callback::new(executor.clone(), move |value: i32, err: Option<()>| {
        assert!(err.is_none());
        seen_callback.lock().unwrap().push(value);
    });

    callback.schedule(7, None);
    let task = executor.tasks.lock().unwrap().pop().unwrap();
    task();

    assert_eq!(*seen.lock().unwrap(), vec![7]);
}
