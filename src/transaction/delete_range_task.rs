use crate::pd::{PdClient, PdRpcClient};
use crate::{BoundRange, Result};

use super::{Client, RangeTaskRunner, RangeTaskStat};

/// Delete all versions of all keys in a range using the transactional API.
///
/// This is a convenience wrapper mirroring client-go `rangetask.DeleteRangeTask`.
///
/// The task executes delete-range requests over the provided key range, in parallel, and tracks
/// the number of affected regions (as reported by TiKV).
///
/// Notes:
/// - This operation is destructive and bypasses MVCC history; use with care.
/// - When `notify_only` is enabled (see [`DeleteRangeTask::new_notify`]), TiKV will replicate the
///   request via Raft but will not actually delete keys. This is typically used before issuing
///   `UnsafeDestroyRange` requests.
pub struct DeleteRangeTask<PdC: PdClient = PdRpcClient> {
    client: Client<PdC>,
    range: BoundRange,
    concurrency: usize,
    notify_only: bool,
    completed_regions: usize,
}

impl<PdC: PdClient> DeleteRangeTask<PdC> {
    /// Create a delete-range task.
    pub fn new(client: Client<PdC>, range: impl Into<BoundRange>, concurrency: usize) -> Self {
        Self {
            client,
            range: range.into(),
            concurrency,
            notify_only: false,
            completed_regions: 0,
        }
    }

    /// Create a notify-only delete-range task.
    ///
    /// This sends delete-range requests with `notify_only=true`.
    pub fn new_notify(
        client: Client<PdC>,
        range: impl Into<BoundRange>,
        concurrency: usize,
    ) -> Self {
        Self {
            client,
            range: range.into(),
            concurrency,
            notify_only: true,
            completed_regions: 0,
        }
    }

    /// Returns whether this task is configured in notify-only mode.
    #[must_use]
    pub fn notify_only(&self) -> bool {
        self.notify_only
    }

    /// Returns how many regions have been reported as completed by this task.
    #[must_use]
    pub fn completed_regions(&self) -> usize {
        self.completed_regions
    }

    /// Execute the delete-range task.
    ///
    /// When this returns an error, [`DeleteRangeTask::completed_regions`] still reflects the number
    /// of successful region tasks completed before the error was observed.
    pub async fn execute(&mut self) -> Result<()> {
        self.completed_regions = 0;

        let runner_name = if self.notify_only {
            "delete-range-notify"
        } else {
            "delete-range"
        };

        let client = self.client.clone();
        let notify_only = self.notify_only;
        let handler = move |range: BoundRange| {
            let client = client.clone();
            async move {
                let res = if notify_only {
                    client.notify_delete_range(range, 1).await
                } else {
                    client.delete_range(range, 1).await
                };

                match res {
                    Ok(completed_regions) => (
                        RangeTaskStat {
                            completed_regions,
                            failed_regions: 0,
                        },
                        Ok(()),
                    ),
                    Err(err) => (
                        RangeTaskStat {
                            completed_regions: 0,
                            failed_regions: 1,
                        },
                        Err(err),
                    ),
                }
            }
        };

        let runner = RangeTaskRunner::new(
            runner_name,
            self.client.pd_client(),
            self.concurrency,
            handler,
        )?;

        let result = runner.run_on_range(self.range.clone()).await;
        self.completed_regions = runner.completed_regions();
        result
    }
}
