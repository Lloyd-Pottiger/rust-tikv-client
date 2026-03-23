mod client;
mod cluster;
mod health_feedback;
mod retry;
mod store_liveness;
mod timestamp;

pub use self::client::PdClient;
pub use self::client::PdRpcClient;
pub(crate) use self::health_feedback::spawn_health_feedback_updater;
pub(crate) use self::store_liveness::spawn_store_liveness_updater;

pub(crate) trait HealthFeedbackObserver: Send + Sync {
    fn observe_health_feedback(&self, feedback: &crate::proto::kvrpcpb::HealthFeedback);
}

#[cfg(test)]
pub(crate) const HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD: i32 =
    self::client::HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD;
#[cfg(test)]
pub(crate) const HEALTH_FEEDBACK_SLOW_STORE_TTL: std::time::Duration =
    self::client::HEALTH_FEEDBACK_SLOW_STORE_TTL;
pub use self::cluster::Cluster;
pub use self::cluster::Connection;
pub use self::retry::RetryClient;
pub use self::retry::RetryClientTrait;
