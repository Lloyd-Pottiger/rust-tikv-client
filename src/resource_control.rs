//! Resource control helpers (test-only).
//!
//! This module ports small, self-contained semantics from client-go's
//! `internal/resourcecontrol` package.

use crate::proto::kvrpcpb;
use crate::store::Request;

const INTERNAL_OTHERS_REQUEST_SOURCE: &str = "internal_others";

fn should_bypass(request_source: &str) -> bool {
    // Align with client-go: bypass some internal request sources (e.g. internal_others).
    request_source.contains(INTERNAL_OTHERS_REQUEST_SOURCE)
}

#[derive(Clone, Debug, Default)]
struct RequestInfo {
    /// -1 for read requests; non-negative for write requests.
    write_bytes: i64,
    store_id: u64,
    bypass: bool,
}

impl RequestInfo {
    fn is_write(&self) -> bool {
        self.write_bytes > -1
    }

    fn write_bytes(&self) -> u64 {
        if self.write_bytes > 0 {
            self.write_bytes as u64
        } else {
            0
        }
    }

    fn bypass(&self) -> bool {
        self.bypass
    }

    fn store_id(&self) -> u64 {
        self.store_id
    }
}

fn make_request_info(req: &mut dyn Request) -> RequestInfo {
    let (store_id, bypass) = {
        let ctx = req.context_mut();
        let store_id = ctx.peer.as_ref().map(|p| p.store_id).unwrap_or(0);
        let bypass = should_bypass(&ctx.request_source);
        (store_id, bypass)
    };

    let write_bytes = if let Some(req) = req.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>() {
        let mut bytes = 0_i64;
        for m in &req.mutations {
            bytes += m.key.len() as i64 + m.value.len() as i64;
        }
        bytes += req.primary_lock.len() as i64;
        for secondary in &req.secondaries {
            bytes += secondary.len() as i64;
        }
        bytes
    } else if let Some(req) = req.as_any().downcast_ref::<kvrpcpb::CommitRequest>() {
        let mut bytes = 0_i64;
        for k in &req.keys {
            bytes += k.len() as i64;
        }
        bytes
    } else {
        -1
    };

    RequestInfo {
        write_bytes,
        store_id,
        bypass,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::metapb;

    #[test]
    fn request_info_matches_client_go_make_request_info_test() {
        // Non-write request.
        let mut req = kvrpcpb::BatchGetRequest {
            context: Some(kvrpcpb::Context {
                peer: Some(metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let info = make_request_info(&mut req);
        assert!(!info.is_write());
        assert_eq!(info.write_bytes(), 0);
        assert!(!info.bypass());
        assert_eq!(info.store_id(), 1);

        // Prewrite request.
        let mutation = kvrpcpb::Mutation {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
            ..Default::default()
        };
        let mut req = kvrpcpb::PrewriteRequest {
            mutations: vec![mutation],
            primary_lock: b"baz".to_vec(),
            context: Some(kvrpcpb::Context {
                peer: Some(metapb::Peer {
                    store_id: 2,
                    ..Default::default()
                }),
                request_source: "xxx_internal_others".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let info = make_request_info(&mut req);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 9);
        assert!(info.bypass());
        assert_eq!(info.store_id(), 2);

        // Commit request.
        let mut req = kvrpcpb::CommitRequest {
            keys: vec![b"qux".to_vec()],
            context: Some(kvrpcpb::Context {
                peer: Some(metapb::Peer {
                    store_id: 3,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let info = make_request_info(&mut req);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 3);
        assert!(!info.bypass());
        assert_eq!(info.store_id(), 3);

        // Nil peer in context.
        let mut req = kvrpcpb::CommitRequest {
            keys: vec![b"qux".to_vec()],
            context: Some(kvrpcpb::Context::default()),
            ..Default::default()
        };
        let info = make_request_info(&mut req);
        assert!(info.is_write());
        assert_eq!(info.write_bytes(), 3);
        assert!(!info.bypass());
        assert_eq!(info.store_id(), 0);
    }
}
