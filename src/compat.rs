// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Internal async/stream helpers.
//!
//! Historically this module existed to smooth a futures 0.1→0.3 migration; today it contains a
//! small helper used to build fallible streams from async state machines.

use futures::stream;
use futures::Future;
use futures::Stream;

/// Create a fallible stream by repeatedly invoking `func` with a state.
///
/// `func` should return an async computation that yields:
/// - `Ok(Some((next_state, item)))` to emit `item` and continue with `next_state`
/// - `Ok(None)` to end the stream
/// - `Err(e)` to emit the error once and then end the stream
pub fn stream_fn<S, T, A, F, E>(initial_state: S, func: F) -> impl Stream<Item = Result<T, E>>
where
    F: FnMut(S) -> A,
    A: Future<Output = Result<Option<(S, T)>, E>>,
{
    stream::unfold(
        (Some(initial_state), func),
        |(state, mut func)| async move {
            let state = state?;

            match func(state).await {
                Ok(Some((next_state, item))) => Some((Ok(item), (Some(next_state), func))),
                Ok(None) => None,
                Err(err) => Some((Err(err), (None, func))),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn stream_fn_emits_error_once_then_ends() {
        let stream = stream_fn(0usize, |state| async move {
            match state {
                0 => Ok(Some((1, "ok"))),
                1 => Err("boom"),
                _ => unreachable!("stream ends after error"),
            }
        });
        futures::pin_mut!(stream);

        assert_eq!(stream.next().await, Some(Ok("ok")));
        assert_eq!(stream.next().await, Some(Err("boom")));
        assert_eq!(stream.next().await, None);
    }
}
