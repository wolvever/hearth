//! ACP catalog: a profile resolves to [`LaunchSpec`] data.
//!
//! Builtin Copilot and Cursor rows live in [`builtin.toml`](builtin.toml).
//! Both extend [`crate::adapters::acp::AcpCodec`] on
//! `jsonrpc-content-length`. This module does not spawn, and it is not a
//! kernel noun or a Queue.
//!
//! Reattach is [`crate::host::HostAttach::resume`] (`native_resume_id`),
//! never Gemini-style `session/load`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::Deserialize;

use crate::adapters::acp::AcpCodec;
use crate::transport::WireKind;
use crate::{BusError, CapabilityFlags};

/// What a catalog row extends. Only generic ACP in this slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogExtends {
    Acp,
}

/// One curated or loaded ACP profile. Not a Binding and not a running process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogProfile {
    pub id: String,
    pub label: String,
    pub command: Vec<String>,
    pub extends: CatalogExtends,
    /// Allowed framed wire. ACP profiles are [`WireKind::JsonRpc`].
    pub wire: WireKind,
    /// Host may set this later. Builtin rows leave it `None` so flags stay
    /// the shared ACP defaults — not a per-vendor codec.
    pub caps_override: Option<CapabilityFlags>,
    pub env: Vec<(String, String)>,
}

/// Spawn inputs for a later runner. Constructing this does not start a process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub wire: WireKind,
}

impl CatalogProfile {
    /// Argv plus wire. No I/O.
    pub fn launch_spec(&self) -> Result<LaunchSpec, BusError> {
        let Some((program, args)) = self.command.split_first() else {
            return Err(BusError::Decode(format!(
                "catalog profile {}: empty command",
                self.id
            )));
        };
        if program.is_empty() {
            return Err(BusError::Decode(format!(
                "catalog profile {}: empty program",
                self.id
            )));
        }
        if WireKind::parse(self.wire.as_str()) != Some(self.wire) {
            return Err(BusError::Decode(format!(
                "catalog profile {}: wire is not an allowed WireKind",
                self.id
            )));
        }
        Ok(LaunchSpec {
            program: program.clone(),
            args: args.to_vec(),
            cwd: None,
            env: self.env.clone(),
            wire: self.wire,
        })
    }

    /// Shared ACP codec. Copilot and Cursor do not get their own adapter.
    pub fn codec(&self) -> AcpCodec {
        match self.extends {
            CatalogExtends::Acp => AcpCodec,
        }
    }
}

#[derive(Debug, Deserialize)]
struct CatalogFile {
    #[serde(default)]
    profile: Vec<RawProfile>,
}

#[derive(Debug, Deserialize)]
struct RawProfile {
    id: String,
    label: String,
    command: Vec<String>,
    extends: String,
    #[serde(default)]
    wire: Option<String>,
    #[serde(default)]
    env: Vec<RawEnv>,
}

#[derive(Debug, Deserialize)]
struct RawEnv {
    key: String,
    value: String,
}

fn parse_extends(id: &str, extends: &str) -> Result<CatalogExtends, BusError> {
    match extends {
        "acp" => Ok(CatalogExtends::Acp),
        other => Err(BusError::Decode(format!(
            "catalog profile {id}: extends {other:?} is not supported (only \"acp\")"
        ))),
    }
}

fn parse_wire(id: &str, extends: CatalogExtends, wire: Option<&str>) -> Result<WireKind, BusError> {
    let kind = match wire {
        None => match extends {
            CatalogExtends::Acp => WireKind::JsonRpc,
        },
        Some(name) => WireKind::parse(name).ok_or_else(|| {
            BusError::Decode(format!(
                "catalog profile {id}: wire {name:?} is not an allowed WireKind"
            ))
        })?,
    };
    if extends == CatalogExtends::Acp && kind != WireKind::JsonRpc {
        return Err(BusError::Decode(format!(
            "catalog profile {id}: acp profiles use {}",
            WireKind::JsonRpc.as_str()
        )));
    }
    Ok(kind)
}

/// Parse catalog TOML. Does not spawn and does not touch the filesystem
/// beyond the caller-supplied text.
pub fn load_profiles_str(text: &str) -> Result<Vec<CatalogProfile>, BusError> {
    let file: CatalogFile =
        toml::from_str(text).map_err(|e| BusError::Decode(format!("catalog toml: {e}")))?;
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(file.profile.len());
    for raw in file.profile {
        if raw.id.is_empty() {
            return Err(BusError::Decode("catalog profile id is empty".into()));
        }
        if !seen.insert(raw.id.clone()) {
            return Err(BusError::Decode(format!(
                "catalog profile {} is duplicated",
                raw.id
            )));
        }
        if raw.command.is_empty() || raw.command.iter().any(|p| p.is_empty()) {
            return Err(BusError::Decode(format!(
                "catalog profile {}: command must be a non-empty argv",
                raw.id
            )));
        }
        let extends = parse_extends(&raw.id, &raw.extends)?;
        let wire = parse_wire(&raw.id, extends, raw.wire.as_deref())?;
        let mut env = Vec::with_capacity(raw.env.len());
        for pair in raw.env {
            if pair.key.is_empty() {
                return Err(BusError::Decode(format!(
                    "catalog profile {}: env key is empty",
                    raw.id
                )));
            }
            env.push((pair.key, pair.value));
        }
        out.push(CatalogProfile {
            id: raw.id,
            label: raw.label,
            command: raw.command,
            extends,
            wire,
            caps_override: None,
            env,
        });
    }
    Ok(out)
}

/// Load a catalog file from disk. Still data only — no process spawn.
pub fn load_profiles(path: &Path) -> Result<Vec<CatalogProfile>, BusError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| BusError::Decode(format!("catalog {}: {e}", path.display())))?;
    load_profiles_str(&text)
}

fn builtin_vec() -> &'static Vec<CatalogProfile> {
    static BUILTIN: OnceLock<Vec<CatalogProfile>> = OnceLock::new();
    BUILTIN.get_or_init(|| {
        load_profiles_str(include_str!("builtin.toml")).expect("catalog/builtin.toml")
    })
}

/// Curated profiles compiled into the crate (`builtin.toml`).
pub fn builtin_profiles() -> &'static [CatalogProfile] {
    builtin_vec()
}

/// Lookup a builtin profile by id (`"copilot"`, `"cursor"`).
pub fn lookup_profile(id: &str) -> Option<&'static CatalogProfile> {
    builtin_profiles().iter().find(|p| p.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::AdapterCodec;
    use crate::transport::WireFrame;
    use crate::{registry, AgentEvent, AgentKind, BusError};

    fn builtin_fixture_text() -> String {
        let path = format!("{}/src/catalog/builtin.toml", env!("CARGO_MANIFEST_DIR"));
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    fn acp_tool_fixture() -> serde_json::Value {
        let path = format!(
            "{}/src/adapters/acp/fixtures/session_update_tool_call.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn builtin_fixture_resolves_copilot_and_cursor_launch_specs() {
        let from_file = load_profiles_str(&builtin_fixture_text()).unwrap();
        assert_eq!(from_file, builtin_profiles());

        let copilot = lookup_profile("copilot").expect("copilot");
        let cursor = lookup_profile("cursor").expect("cursor");
        assert_eq!(copilot.label, "GitHub Copilot");
        assert_eq!(cursor.label, "Cursor");
        assert_eq!(copilot.extends, CatalogExtends::Acp);
        assert_eq!(cursor.extends, CatalogExtends::Acp);
        assert!(copilot.caps_override.is_none());
        assert!(cursor.caps_override.is_none());

        let copilot_spec = copilot.launch_spec().unwrap();
        assert_eq!(
            copilot_spec,
            LaunchSpec {
                program: "copilot".into(),
                args: vec!["--acp".into()],
                cwd: None,
                env: vec![],
                wire: WireKind::JsonRpc,
            }
        );
        let cursor_spec = cursor.launch_spec().unwrap();
        assert_eq!(
            cursor_spec,
            LaunchSpec {
                program: "cursor-agent".into(),
                args: vec!["acp".into()],
                cwd: None,
                env: vec![],
                wire: WireKind::JsonRpc,
            }
        );
        assert_eq!(copilot_spec.wire.as_str(), "jsonrpc-content-length");
        assert_eq!(cursor_spec.wire.as_str(), "jsonrpc-content-length");
        assert!(WireKind::parse(copilot_spec.wire.as_str()).is_some());
        assert!(WireKind::parse(cursor_spec.wire.as_str()).is_some());
    }

    #[test]
    fn copilot_and_cursor_share_acp_codec_on_fixture() {
        let frame = WireFrame::Json(acp_tool_fixture());
        let copilot = lookup_profile("copilot").unwrap().codec();
        let cursor = lookup_profile("cursor").unwrap().codec();
        assert_eq!(copilot.kind(), AgentKind::Acp);
        assert_eq!(cursor.kind(), AgentKind::Acp);
        assert_eq!(copilot.wire(), cursor.wire());
        assert_eq!(copilot.wire(), WireKind::JsonRpc.as_str());
        let left = copilot.decode_event(&frame).unwrap();
        let right = cursor.decode_event(&frame).unwrap();
        assert_eq!(left, right);
        assert!(matches!(
            left,
            Some(AgentEvent::ToolCall { ref item_id, ref name, .. })
                if item_id == "c1" && name == "read"
        ));
    }

    #[test]
    fn registry_and_profile_wires_stay_allowed() {
        for info in registry() {
            assert!(
                info.wire_kind().is_some(),
                "{} wire {:?} is not an allowed WireKind",
                info.name,
                info.wire
            );
        }
        for profile in builtin_profiles() {
            let spec = profile.launch_spec().unwrap();
            assert_eq!(WireKind::parse(spec.wire.as_str()), Some(spec.wire));
            assert_eq!(
                WireKind::parse(profile.codec().wire()),
                Some(WireKind::JsonRpc)
            );
        }
    }

    #[test]
    fn resume_not_load_profiles_do_not_speak_session_load() {
        for id in ["copilot", "cursor"] {
            let spec = lookup_profile(id).unwrap().launch_spec().unwrap();
            let argv =
                std::iter::once(spec.program.as_str()).chain(spec.args.iter().map(String::as_str));
            for arg in argv {
                assert!(
                    !arg.contains("load"),
                    "{id} argv must not request session/load ({arg})"
                );
            }
        }
        // OpenSession stays unsupported on the shared codec — no session/load encode.
        let frame = AcpCodec.encode_command(&crate::AgentCommand::OpenSession {
            project_id: "p".into(),
            cwd: None,
        });
        assert!(matches!(frame, Err(BusError::Unsupported("OpenSession"))));
        let prompt = AcpCodec
            .encode_command(&crate::AgentCommand::UserMessage {
                session_id: "s".into(),
                text: "hi".into(),
            })
            .unwrap();
        let method = prompt
            .as_json()
            .and_then(|v| v.get("method"))
            .and_then(|m| m.as_str());
        assert_ne!(method, Some("session/load"));
        assert_eq!(method, Some("session/prompt"));
    }

    #[test]
    fn unknown_profile_and_bad_extends_are_rejected() {
        assert!(lookup_profile("kimi").is_none());
        let err = load_profiles_str(
            r#"
            [[profile]]
            id = "custom"
            label = "Custom"
            command = ["my-agent", "--stdio-acp"]
            extends = "gemini"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, BusError::Decode(_)));
        let err = load_profiles_str(
            r#"
            [[profile]]
            id = "badwire"
            label = "Bad"
            command = ["agent", "acp"]
            extends = "acp"
            wire = "ndjson"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, BusError::Decode(_)));
    }
}
