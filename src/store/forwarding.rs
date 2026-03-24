use std::future::Future;

use tonic::metadata::MetadataValue;

use crate::Error;
use crate::Result;

pub(crate) const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";

tokio::task_local! {
    static TASK_FORWARDED_HOST: String;
}

pub(crate) fn forwarded_host() -> Option<String> {
    TASK_FORWARDED_HOST
        .try_with(|host| host.clone())
        .ok()
        .filter(|host| !host.is_empty())
}

pub(crate) async fn scope_forwarded_host<T>(
    forwarded_host: String,
    fut: impl Future<Output = T>,
) -> T {
    TASK_FORWARDED_HOST.scope(forwarded_host, fut).await
}

pub(crate) fn apply_forwarded_host_metadata_value<T>(
    req: &mut tonic::Request<T>,
    forwarded_host: &str,
) -> Result<()> {
    if forwarded_host.is_empty() {
        return Ok(());
    }
    let value: MetadataValue<_> = forwarded_host.parse().map_err(|err| {
        Error::StringError(format!("invalid forwarded host metadata value: {err}"))
    })?;
    req.metadata_mut().insert(FORWARD_METADATA_KEY, value);
    Ok(())
}

pub(crate) fn apply_forwarded_host_metadata<T>(req: &mut tonic::Request<T>) -> Result<()> {
    let Ok(forwarded_host) = TASK_FORWARDED_HOST.try_with(|host| host.clone()) else {
        return Ok(());
    };
    apply_forwarded_host_metadata_value(req, &forwarded_host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_apply_forwarded_host_metadata_sets_header() {
        let mut req = tonic::Request::new(());
        apply_forwarded_host_metadata(&mut req).unwrap();
        assert!(req.metadata().get(FORWARD_METADATA_KEY).is_none());

        scope_forwarded_host("127.0.0.1:20160".to_owned(), async {
            let mut req = tonic::Request::new(());
            apply_forwarded_host_metadata(&mut req).unwrap();
            let value = req
                .metadata()
                .get(FORWARD_METADATA_KEY)
                .expect("expected forwarded host metadata");
            assert_eq!(value.to_str().unwrap(), "127.0.0.1:20160");
        })
        .await;
    }
}
