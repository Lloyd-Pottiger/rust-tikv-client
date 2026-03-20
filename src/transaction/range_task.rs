use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::TryStreamExt;
use tokio::sync::mpsc;

use crate::compat::stream_fn;
use crate::pd::PdClient;
use crate::stats::{add_range_task_stats, observe_range_task_push_duration, set_range_task_stats};
use crate::store::region_stream_for_range;
use crate::{BoundRange, Error, Result};

type RegionRangesStream = futures::stream::BoxStream<
    'static,
    Result<((Vec<u8>, Vec<u8>), crate::region::RegionWithLeader)>,
>;

/// Statistics reported by a range task.
///
/// This mirrors client-go `rangetask.TaskStat`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeTaskStat {
    pub completed_regions: usize,
    pub failed_regions: usize,
}

/// A handler invoked for each sub-range produced by [`RangeTaskRunner`].
///
/// This mirrors client-go `rangetask.TaskHandler`, returning a `(stat, error)` pair so the
/// runner can aggregate stats even when a task fails.
#[async_trait]
pub trait RangeTaskHandler: Send + Sync + 'static {
    async fn handle(&self, range: BoundRange) -> (RangeTaskStat, Result<()>);
}

#[async_trait]
impl<F, Fut> RangeTaskHandler for F
where
    F: Fn(BoundRange) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = (RangeTaskStat, Result<()>)> + Send,
{
    async fn handle(&self, range: BoundRange) -> (RangeTaskStat, Result<()>) {
        (self)(range).await
    }
}

/// Runs a user-provided handler over a key range by splitting it into per-region subranges.
///
/// This mirrors client-go `txnkv/rangetask.Runner`.
pub struct RangeTaskRunner<PdC: PdClient> {
    identifier: String,
    pd_client: Arc<PdC>,
    concurrency: usize,
    regions_per_task: usize,
    handler: Arc<dyn RangeTaskHandler>,
    completed_regions: AtomicUsize,
    failed_regions: AtomicUsize,
}

impl<PdC: PdClient> RangeTaskRunner<PdC> {
    /// Create a range task runner.
    ///
    /// Returns an error if `concurrency == 0`.
    pub fn new(
        identifier: impl Into<String>,
        pd_client: Arc<PdC>,
        concurrency: usize,
        handler: impl RangeTaskHandler,
    ) -> Result<Self> {
        if concurrency == 0 {
            return Err(Error::StringError(
                "range task runner concurrency must be greater than 0".to_owned(),
            ));
        }

        Ok(Self {
            identifier: identifier.into(),
            pd_client,
            concurrency,
            regions_per_task: 1,
            handler: Arc::new(handler),
            completed_regions: AtomicUsize::new(0),
            failed_regions: AtomicUsize::new(0),
        })
    }

    /// Update how many regions to include in each task range.
    ///
    /// Returns an error if `regions_per_task == 0`.
    pub fn set_regions_per_task(&mut self, regions_per_task: usize) -> Result<()> {
        if regions_per_task == 0 {
            return Err(Error::StringError(
                "range task runner regions_per_task must be greater than 0".to_owned(),
            ));
        }
        self.regions_per_task = regions_per_task;
        Ok(())
    }

    /// Returns how many regions have been reported as completed by the handler.
    #[must_use]
    pub fn completed_regions(&self) -> usize {
        self.completed_regions.load(Ordering::Relaxed)
    }

    /// Returns how many regions have been reported as failed by the handler.
    #[must_use]
    pub fn failed_regions(&self) -> usize {
        self.failed_regions.load(Ordering::Relaxed)
    }

    /// Returns the runner identifier (primarily for debugging/logging).
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Runs the task on the given range.
    ///
    /// Empty start key or end key means unbounded (matching [`BoundRange`] conventions).
    ///
    /// Returns `Ok(())` only when all sub tasks finish successfully.
    pub async fn run_on_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        self.completed_regions.store(0, Ordering::Relaxed);
        self.failed_regions.store(0, Ordering::Relaxed);

        let identifier = self.identifier.as_str();
        set_range_task_stats(identifier, 0, 0);
        let _reset = RangeTaskStatsReset { identifier };

        let range = range.into();
        let (start_key, end_key) = range.into_keys();
        let start_key = Vec::<u8>::from(start_key);
        let end_key = end_key.map(Vec::<u8>::from).unwrap_or_default();

        if !end_key.is_empty() && start_key >= end_key {
            return Ok(());
        }

        let pd_client = self.pd_client.clone();
        let regions_per_task = self.regions_per_task;

        let task_ranges = stream_fn(
            TaskRangesState {
                region_stream: region_stream_for_range((start_key, end_key), pd_client),
                regions_per_task,
                current_start: None,
                current_end: Vec::new(),
                current_region_count: 0,
                drain_final: false,
            },
            |mut state| async move {
                if state.drain_final {
                    return Ok(None);
                }

                loop {
                    let next = state.region_stream.next().await.transpose()?;
                    match next {
                        None => {
                            state.drain_final = true;
                            if let Some(start) = state.current_start.take() {
                                let end = std::mem::take(&mut state.current_end);
                                let range = BoundRange::from(start..end);
                                return Ok(Some((state, range)));
                            }
                            return Ok(None);
                        }
                        Some(((segment_start, segment_end), _region)) => {
                            if state.current_start.is_none() {
                                state.current_start = Some(segment_start);
                            }
                            state.current_end = segment_end;
                            state.current_region_count += 1;

                            if state.current_region_count >= state.regions_per_task {
                                let Some(start) = state.current_start.take() else {
                                    return Err(Error::StringError(
                                        "range task runner missing range start key".to_owned(),
                                    ));
                                };
                                let end = std::mem::take(&mut state.current_end);
                                state.current_region_count = 0;
                                let range = BoundRange::from(start..end);
                                return Ok(Some((state, range)));
                            }
                        }
                    }
                }
            },
        );

        let (tx, rx) = mpsc::channel::<BoundRange>(self.concurrency);

        let producer = async move {
            futures::pin_mut!(task_ranges);
            let tx = tx;

            while let Some(range) = task_ranges.try_next().await? {
                let started_at = std::time::Instant::now();
                if tx.send(range).await.is_err() {
                    // Consumer is no longer interested (likely aborted due to error).
                    break;
                }
                observe_range_task_push_duration(identifier, started_at.elapsed());
            }

            Ok(())
        };

        let handler = self.handler.clone();
        let completed_regions = &self.completed_regions;
        let failed_regions = &self.failed_regions;

        let consumer_stream = stream_fn(rx, |mut rx| async move {
            Ok::<_, crate::Error>(match rx.recv().await {
                Some(range) => Some((rx, range)),
                None => None,
            })
        });

        let consumer = consumer_stream.try_for_each_concurrent(self.concurrency, move |range| {
            let handler = handler.clone();
            async move {
                let (stat, res) = handler.handle(range).await;

                completed_regions.fetch_add(stat.completed_regions, Ordering::Relaxed);
                failed_regions.fetch_add(stat.failed_regions, Ordering::Relaxed);
                add_range_task_stats(identifier, stat.completed_regions, stat.failed_regions);

                res.map_err(|err| match err {
                    Error::StringError(message) => Error::StringError(format!(
                        "range task runner {identifier} failed: {message}"
                    )),
                    other => other,
                })?;
                Ok(())
            }
        });

        let ((), ()) = tokio::try_join!(producer, consumer)?;
        Ok(())
    }
}

struct RangeTaskStatsReset<'a> {
    identifier: &'a str,
}

impl Drop for RangeTaskStatsReset<'_> {
    fn drop(&mut self) {
        set_range_task_stats(self.identifier, 0, 0);
    }
}

struct TaskRangesState {
    region_stream: RegionRangesStream,
    regions_per_task: usize,
    current_start: Option<Vec<u8>>,
    current_end: Vec<u8>,
    current_region_count: usize,
    drain_final: bool,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use serial_test::serial;
    use tokio::sync::Mutex;

    use super::{RangeTaskRunner, RangeTaskStat};
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::BoundRange;

    #[tokio::test]
    async fn test_range_task_runner_splits_ranges_by_regions_per_task() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::default()));

        let ranges_seen = Arc::new(Mutex::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
        let ranges_seen_captured = ranges_seen.clone();

        let handler = move |range: BoundRange| {
            let ranges_seen = ranges_seen_captured.clone();
            async move {
                let (start, end) = range.into_keys();
                let start = Vec::<u8>::from(start);
                let end = end.map(Vec::<u8>::from).unwrap_or_default();
                ranges_seen.lock().await.push((start, end));
                (
                    RangeTaskStat {
                        completed_regions: 1,
                        failed_regions: 0,
                    },
                    Ok(()),
                )
            }
        };

        let mut runner = RangeTaskRunner::new("test", pd_client, 2, handler).unwrap();

        for (regions_per_task, expected) in [
            (
                1,
                vec![
                    (vec![], vec![10]),
                    (vec![10], vec![250, 250]),
                    (vec![250, 250], vec![]),
                ],
            ),
            (2, vec![(vec![], vec![250, 250]), (vec![250, 250], vec![])]),
            (3, vec![(vec![], vec![])]),
            (5, vec![(vec![], vec![])]),
        ] {
            runner.set_regions_per_task(regions_per_task).unwrap();
            runner.run_on_range(..).await.unwrap();

            let mut actual = ranges_seen.lock().await.clone();
            actual.sort();
            let mut expected = expected;
            expected.sort();
            assert_eq!(actual, expected);
            assert_eq!(runner.completed_regions(), expected.len());
            assert_eq!(runner.failed_regions(), 0);

            ranges_seen.lock().await.clear();
        }
    }

    #[tokio::test]
    async fn test_range_task_runner_empty_range_is_noop() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::default()));

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();
        let handler = move |_range: BoundRange| {
            let calls = calls_captured.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                (RangeTaskStat::default(), Ok(()))
            }
        };

        let runner = RangeTaskRunner::new("test", pd_client, 1, handler).unwrap();
        runner.run_on_range(vec![10]..vec![10]).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(runner.completed_regions(), 0);
        assert_eq!(runner.failed_regions(), 0);
    }

    #[tokio::test]
    async fn test_range_task_runner_stops_on_error_and_reports_stats() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::default()));

        let handler = move |range: BoundRange| async move {
            let (start, end) = range.into_keys();
            let start = Vec::<u8>::from(start);
            let end = end.map(Vec::<u8>::from).unwrap_or_default();

            if start == vec![10] && end == vec![250, 250] {
                (
                    RangeTaskStat {
                        completed_regions: 0,
                        failed_regions: 1,
                    },
                    Err(crate::Error::StringError("injected error".to_owned())),
                )
            } else {
                (
                    RangeTaskStat {
                        completed_regions: 1,
                        failed_regions: 0,
                    },
                    Ok(()),
                )
            }
        };

        let mut runner = RangeTaskRunner::new("test", pd_client, 1, handler).unwrap();
        runner.set_regions_per_task(1).unwrap();

        let err = runner.run_on_range(..).await.unwrap_err();
        match err {
            crate::Error::StringError(message) => {
                assert!(message.contains("injected error"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(runner.completed_regions() < 3);
        assert_eq!(runner.failed_regions(), 1);
    }

    #[tokio::test]
    async fn test_range_task_runner_validation_rejects_invalid_parameters() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::default()));

        let handler = |_range: BoundRange| async move { (RangeTaskStat::default(), Ok(())) };

        assert!(RangeTaskRunner::new("test", pd_client.clone(), 0, handler).is_err());

        let mut runner = RangeTaskRunner::new("test", pd_client, 1, handler).unwrap();
        assert!(runner.set_regions_per_task(0).is_err());
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_range_task_runner_records_range_task_metrics_and_resets_gauge() {
        fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
            metric
                .get_label()
                .iter()
                .find(|pair| pair.get_name() == name)
                .map(|pair| pair.get_value())
        }

        fn range_task_completed_gauge_value(task: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_range_task_stats")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(task)
                            && label_value(metric, "result") == Some("completed-regions")
                            && metric.has_gauge()
                    })
                })
                .map(|metric| metric.get_gauge().get_value())
                .unwrap_or(0.0)
        }

        fn range_task_push_duration_sample_count(task: &str) -> u64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_range_task_push_duration")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(task) && metric.has_histogram()
                    })
                })
                .map(|metric| metric.get_histogram().get_sample_count())
                .unwrap_or(0)
        }

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::default()));
        let task_label = "range-task-metrics-test";

        let handler = |_range: BoundRange| async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            (
                RangeTaskStat {
                    completed_regions: 1,
                    failed_regions: 0,
                },
                Ok(()),
            )
        };

        let runner = RangeTaskRunner::new(task_label, pd_client, 1, handler).unwrap();

        let monitor = async {
            for _ in 0..50 {
                if runner.completed_regions() >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            assert!(
                range_task_completed_gauge_value(task_label) >= 1.0,
                "expected completed-regions gauge to be updated during execution"
            );
            assert!(
                range_task_push_duration_sample_count(task_label) >= 1,
                "expected push duration histogram to record samples"
            );
        };

        let (run_res, ()) = tokio::join!(runner.run_on_range(..), monitor);
        run_res.unwrap();

        assert!(
            (range_task_completed_gauge_value(task_label) - 0.0).abs() < 1e-6,
            "expected completed-regions gauge to be reset to 0 after execution"
        );
        assert_eq!(runner.completed_regions(), 3);
        assert_eq!(runner.failed_regions(), 0);
    }
}
