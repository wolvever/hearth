//! Grok Build — Agent Client Protocol (ACP) JSON-RPC.
//! Source: xai-org/grok-build (`grok agent stdio` / `serve`).
//!
//! Session/update + permission mapping is shared with [`crate::adapters::acp`].
//! This folder keeps [`GrokBuildCodec`] / [`AgentKind::GrokBuild`].

use crate::adapters::acp;
use crate::adapters::AdapterCodec;
use crate::transport::WireFrame;
use crate::{AgentCommand, AgentEvent, AgentKind, BusError};

pub fn map_notification(msg: &serde_json::Value) -> Result<AgentEvent, BusError> {
    acp::map_notification(msg, AgentKind::GrokBuild)
}

/// ACP codec for Grok Build. Consumes JSON-RPC [`WireFrame::Json`] only.
#[derive(Debug, Default, Clone, Copy)]
pub struct GrokBuildCodec;

impl AdapterCodec for GrokBuildCodec {
    fn kind(&self) -> AgentKind {
        AgentKind::GrokBuild
    }

    fn wire(&self) -> &'static str {
        "jsonrpc-content-length"
    }

    fn decode_event(&self, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError> {
        match frame {
            WireFrame::Json(v) => map_notification(v).map(Some),
            WireFrame::Sse { .. } => Err(BusError::Decode(
                "grok_build expects JSON-RPC frames, not SSE".into(),
            )),
        }
    }

    fn encode_command(&self, cmd: &AgentCommand) -> Result<WireFrame, BusError> {
        acp::encode_frame(cmd)
    }
}

#[cfg(test)]
mod map_tests {
    use super::*;
    use crate::adapters::AdapterCodec;
    use crate::transport::WireFrame;
    use crate::ToolStatus;

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/src/adapters/grok_build/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn map_tool_call_fixture() {
        let ev = map_notification(&fixture("session_update_tool_call.json")).unwrap();
        match ev {
            AgentEvent::ToolCall {
                item_id,
                name,
                status,
                ..
            } => {
                assert_eq!(item_id, "c1");
                assert_eq!(name, "read");
                assert_eq!(status, ToolStatus::Pending);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_permission_fixture() {
        let ev = map_notification(&fixture("permission_request.json")).unwrap();
        assert!(matches!(ev, AgentEvent::PermissionAsk { .. }));
    }

    #[test]
    fn codec_rejects_sse_frame() {
        let frame = WireFrame::Sse {
            event: None,
            id: None,
            data: serde_json::json!({}),
        };
        assert!(GrokBuildCodec.decode_event(&frame).is_err());
    }

    #[test]
    fn native_unmapped_keeps_grok_build_kind() {
        let ev = map_notification(&serde_json::json!({
            "method": "session/update",
            "params": {
                "sessionId": "s1",
                "update": { "sessionUpdate": "unknown_kind" }
            }
        }))
        .unwrap();
        match ev {
            AgentEvent::Native { agent, .. } => assert_eq!(agent, AgentKind::GrokBuild),
            other => panic!("unexpected {other:?}"),
        }
    }
}
