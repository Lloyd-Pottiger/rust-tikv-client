use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::requests::new_flush_request;
use super::requests::new_resolve_lock_request;
use super::PipelinedTxnOptions;
use crate::proto::kvrpcpb;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::request::RetryOptions;
use crate::Key;
use crate::RequestContext;
use crate::Result;

/// The fixed request source used by pipelined flush / resolve-lock requests (client-go v2 parity).
pub(crate) const PIPELINED_REQUEST_SOURCE: &str = "external_pdml";

/// The lock TTL used by pipelined flush requests.
///
/// Matches client-go's `max(defaultLockTTL, ManagedLockTTL)` which currently resolves to 20s.
const PIPELINED_LOCK_TTL_MS: u64 = 20_000;

pub(crate) struct PipelinedTxnState {
    opts: PipelinedTxnOptions,
    generation: u64,
    mutable: BTreeMap<Key, kvrpcpb::Mutation>,
    flushing: Option<JoinHandle<Result<()>>>,

    flushed_start: Option<Key>,
    flushed_end_exclusive: Option<Key>,

    cancelled: Arc<AtomicBool>,
    cancel_notify: Arc<Notify>,
}

impl PipelinedTxnState {
    pub(crate) fn new(opts: PipelinedTxnOptions) -> Self {
        Self {
            opts,
            generation: 0,
            mutable: BTreeMap::new(),
            flushing: None,
            flushed_start: None,
            flushed_end_exclusive: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            cancel_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) async fn resolve_flushed_locks<PdC: crate::pd::PdClient>(
        pd_client: Arc<PdC>,
        keyspace: Keyspace,
        request_context: RequestContext,
        start_ts: u64,
        commit_ts: u64,
        range: (Vec<u8>, Vec<u8>),
        concurrency: usize,
    ) -> Result<()> {
        let mut stores = Vec::new();
        let mut key = range.0;
        let end = range.1;
        loop {
            if !end.is_empty() && key >= end {
                break;
            }
            let region = pd_client.region_for_key(&Key::from(key.clone())).await?;
            let next = region.region.end_key.clone();
            let store = pd_client.clone().map_region_to_store(region).await?;
            stores.push(store);

            if next.is_empty() {
                break;
            }
            if !end.is_empty() && next >= end {
                break;
            }
            key = next;
        }

        let sem = Arc::new(Semaphore::new(concurrency.max(1)));
        let mut handles = Vec::with_capacity(stores.len());
        for store in stores {
            let sem = sem.clone();
            let pd_client = pd_client.clone();
            let request_context = request_context.clone();
            let permit = sem.acquire_owned().await.unwrap();
            let handle = tokio::spawn(async move {
                let _permit = permit;
                let request =
                    request_context.apply_to(new_resolve_lock_request(start_ts, commit_ts));
                let plan = PlanBuilder::new(pd_client, keyspace, request)
                    .with_request_context(request_context.clone())
                    .single_region_with_store(store)
                    .await?
                    .extract_error()
                    .plan();
                let _ = plan.execute().await?;
                Ok::<(), crate::Error>(())
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await??;
        }
        Ok(())
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.cancel_notify.notify_waiters();
    }

    pub(crate) fn record_mutation(&mut self, key: Key, mutation: Option<kvrpcpb::Mutation>) {
        match mutation {
            Some(m) => {
                self.mutable.insert(key, m);
            }
            None => {
                self.mutable.remove(&key);
            }
        }
    }

    pub(crate) async fn maybe_flush<PdC: crate::pd::PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
        keyspace: Keyspace,
        start_ts: u64,
        primary_key: Vec<u8>,
        request_context: &RequestContext,
        retry_options: &RetryOptions,
    ) -> Result<()> {
        if self.flushing.is_some() {
            return Ok(());
        }
        if self.mutable.len() < self.opts.min_flush_keys() {
            return Ok(());
        }
        self.start_flush(
            pd_client,
            keyspace,
            start_ts,
            primary_key,
            request_context,
            retry_options,
        )
        .await
    }

    pub(crate) async fn flush_all<PdC: crate::pd::PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
        keyspace: Keyspace,
        start_ts: u64,
        primary_key: Vec<u8>,
        request_context: &RequestContext,
        retry_options: &RetryOptions,
    ) -> Result<()> {
        loop {
            self.flush_wait().await?;
            if self.mutable.is_empty() {
                return Ok(());
            }
            self.start_flush(
                pd_client.clone(),
                keyspace,
                start_ts,
                primary_key.clone(),
                request_context,
                retry_options,
            )
            .await?;
        }
    }

    pub(crate) async fn flush_wait(&mut self) -> Result<()> {
        let Some(handle) = self.flushing.take() else {
            return Ok(());
        };
        handle.await?
    }

    pub(crate) fn flushed_range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let start = self.flushed_start.clone()?;
        let end = self.flushed_end_exclusive.clone()?;
        Some((start.into(), end.into()))
    }

    async fn start_flush<PdC: crate::pd::PdClient>(
        &mut self,
        pd_client: Arc<PdC>,
        keyspace: Keyspace,
        start_ts: u64,
        primary_key: Vec<u8>,
        request_context: &RequestContext,
        retry_options: &RetryOptions,
    ) -> Result<()> {
        self.flush_wait().await?;
        if self.mutable.is_empty() {
            return Ok(());
        }
        self.generation = self.generation.saturating_add(1);
        let generation = self.generation;

        // Track the global flushed key range as [min_key, max_key.next()).
        let min_key = self
            .mutable
            .keys()
            .next()
            .expect("mutable is not empty")
            .clone();
        let max_key = self
            .mutable
            .keys()
            .next_back()
            .expect("mutable is not empty")
            .clone();
        self.update_flushed_range(min_key, max_key);

        let mutations = std::mem::take(&mut self.mutable)
            .into_iter()
            .map(|(_, m)| m)
            .collect::<Vec<_>>();

        let pipelined_ctx = request_context.with_request_source(PIPELINED_REQUEST_SOURCE);
        let cancelled = self.cancelled.clone();
        let cancel_notify = self.cancel_notify.clone();
        let lock_backoff = retry_options.lock_backoff.clone();
        let region_backoff = retry_options.region_backoff.clone();
        let flush_concurrency = self.opts.flush_concurrency_limit();

        let handle = tokio::spawn(async move {
            if cancelled.load(Ordering::Acquire) {
                return Ok(());
            }

            let request = new_flush_request(
                mutations,
                primary_key,
                start_ts,
                generation,
                PIPELINED_LOCK_TTL_MS,
            );
            let request = pipelined_ctx.apply_to(request);
            let plan = PlanBuilder::new(pd_client, keyspace, request)
                .with_request_context(pipelined_ctx.clone())
                .resolve_lock(lock_backoff, keyspace)
                .retry_multi_region_with_concurrency(region_backoff, flush_concurrency)
                .extract_error()
                .plan();

            tokio::select! {
                _ = cancel_notify.notified() => Ok(()),
                res = plan.execute() => {
                    let _ = res?;
                    Ok(())
                }
            }
        });
        self.flushing = Some(handle);
        Ok(())
    }

    fn update_flushed_range(&mut self, min_key: Key, max_key: Key) {
        let max_key_exclusive = max_key.clone().next_key();

        match &self.flushed_start {
            Some(existing) if existing <= &min_key => {}
            _ => self.flushed_start = Some(min_key),
        }
        match &self.flushed_end_exclusive {
            Some(existing) if existing >= &max_key_exclusive => {}
            _ => self.flushed_end_exclusive = Some(max_key_exclusive),
        }
    }
}
