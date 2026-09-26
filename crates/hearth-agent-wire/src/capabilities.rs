//! Typed adapter capability flags.
//!
//! [`CapabilityFlags`] is the compile-checked row on each [`crate::AdapterInfo`].
//! [`CAPABILITIES.md`](../CAPABILITIES.md) documents the same matrix; the unit
//! test in this module fails if a registry row drifts from that file.

use crate::AgentKind;
use serde::{Deserialize, Serialize};

/// Paseo-like + wire-surface flags Host / catalog consult.
///
/// Landed matrix columns (`session`, `message`, `tool`, …) describe events and
/// commands this crate already maps. Reserved Paseo fields stay `false` until
/// later slices (catalog, SetMode, MCP, rewind, session listing).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityFlags {
    /// Token / chunk streaming when the wire emits incremental deltas.
    pub streaming: bool,
    /// Native session can be resumed across attach (later runner / catalog).
    pub session_persistence: bool,
    /// Provider can list existing sessions (later ProviderDriver).
    pub session_listing: bool,
    /// Dynamic mode switching (`SetMode` — slice 2).
    pub dynamic_modes: bool,
    /// MCP server configure (`ConfigureMcp` — slice 2).
    pub mcp_servers: bool,
    /// Thinking / reasoning events (CAPABILITIES `thinking`).
    pub reasoning_stream: bool,
    /// Tool call / result events (CAPABILITIES `tool`).
    pub tool_invocations: bool,
    /// Revert conversation only (`RevertConversation` — later).
    pub rewind_conversation: bool,
    /// Revert files only (`RevertFiles` — later).
    pub rewind_files: bool,
    /// Revert conversation + files (`RevertBoth` — later).
    pub rewind_both: bool,
    /// Session start / end events mapped (CAPABILITIES `session`).
    pub session: bool,
    /// Message / message-delta events mapped (CAPABILITIES `message`).
    pub message: bool,
    pub plan: bool,
    pub permission: bool,
    pub question: bool,
    pub compact: bool,
    pub subagent: bool,
}

impl CapabilityFlags {
    /// Canonical field names. Each must appear as a `CAPABILITIES.md` column
    /// (short aliases `tool`, `thinking`, `subagent/task` count).
    pub const CANONICAL_NAMES: &'static [&'static str] = &[
        "streaming",
        "session_persistence",
        "session_listing",
        "dynamic_modes",
        "mcp_servers",
        "reasoning_stream",
        "tool_invocations",
        "rewind_conversation",
        "rewind_files",
        "rewind_both",
        "session",
        "message",
        "plan",
        "permission",
        "question",
        "compact",
        "subagent",
    ];

    /// Generic ACP / catalog-profile defaults. Compact encode is a stub
    /// (flag stays false). Same surface as [`Self::GROK_BUILD`].
    pub const ACP: Self = Self {
        streaming: false,
        session_persistence: false,
        session_listing: false,
        dynamic_modes: false,
        mcp_servers: false,
        reasoning_stream: true,
        tool_invocations: true,
        rewind_conversation: false,
        rewind_files: false,
        rewind_both: false,
        session: true,
        message: true,
        plan: true,
        permission: true,
        question: false,
        compact: false,
        subagent: false,
    };

    /// Grok Build is an ACP profile — same landed flags as [`Self::ACP`].
    pub const GROK_BUILD: Self = Self::ACP;

    /// Codex App Server. Permission encode is a stub (flag stays false).
    pub const CODEX: Self = Self {
        streaming: false,
        session_persistence: false,
        session_listing: false,
        dynamic_modes: false,
        mcp_servers: false,
        reasoning_stream: true,
        tool_invocations: true,
        rewind_conversation: false,
        rewind_files: false,
        rewind_both: false,
        session: true,
        message: true,
        plan: false,
        permission: false,
        question: false,
        compact: true,
        subagent: false,
    };

    pub const PI: Self = Self {
        streaming: false,
        session_persistence: false,
        session_listing: false,
        dynamic_modes: false,
        mcp_servers: false,
        reasoning_stream: false,
        tool_invocations: true,
        rewind_conversation: false,
        rewind_files: false,
        rewind_both: false,
        session: true,
        message: true,
        plan: false,
        permission: false,
        question: false,
        compact: false,
        subagent: true,
    };

    pub const OPENCODE: Self = Self {
        streaming: false,
        session_persistence: false,
        session_listing: false,
        dynamic_modes: false,
        mcp_servers: false,
        reasoning_stream: true,
        tool_invocations: true,
        rewind_conversation: false,
        rewind_files: false,
        rewind_both: false,
        session: true,
        message: true,
        plan: false,
        permission: true,
        question: true,
        compact: true,
        subagent: false,
    };

    pub const fn for_agent(kind: AgentKind) -> Self {
        match kind {
            AgentKind::GrokBuild => Self::GROK_BUILD,
            AgentKind::Codex => Self::CODEX,
            AgentKind::Pi => Self::PI,
            AgentKind::OpenCode => Self::OPENCODE,
            AgentKind::Acp => Self::ACP,
        }
    }

    /// Look up a flag by CAPABILITIES column or canonical field name.
    pub fn get(self, name: &str) -> Option<bool> {
        Some(match canonical_flag_name(name)? {
            "streaming" => self.streaming,
            "session_persistence" => self.session_persistence,
            "session_listing" => self.session_listing,
            "dynamic_modes" => self.dynamic_modes,
            "mcp_servers" => self.mcp_servers,
            "reasoning_stream" => self.reasoning_stream,
            "tool_invocations" => self.tool_invocations,
            "rewind_conversation" => self.rewind_conversation,
            "rewind_files" => self.rewind_files,
            "rewind_both" => self.rewind_both,
            "session" => self.session,
            "message" => self.message,
            "plan" => self.plan,
            "permission" => self.permission,
            "question" => self.question,
            "compact" => self.compact,
            "subagent" => self.subagent,
            _ => return None,
        })
    }
}

/// Map a markdown column / alias to a [`CapabilityFlags`] field name.
fn canonical_flag_name(raw: &str) -> Option<&'static str> {
    match normalize_header(raw).as_str() {
        "streaming" => Some("streaming"),
        "session_persistence" => Some("session_persistence"),
        "session_listing" => Some("session_listing"),
        "dynamic_modes" => Some("dynamic_modes"),
        "mcp_servers" => Some("mcp_servers"),
        "thinking" | "reasoning_stream" => Some("reasoning_stream"),
        "tool" | "tool_invocations" => Some("tool_invocations"),
        "rewind_conversation" => Some("rewind_conversation"),
        "rewind_files" => Some("rewind_files"),
        "rewind_both" => Some("rewind_both"),
        "session" => Some("session"),
        "message" => Some("message"),
        "plan" => Some("plan"),
        "permission" => Some("permission"),
        "question" => Some("question"),
        "compact" => Some("compact"),
        "subagent" | "subagent/task" => Some("subagent"),
        _ => None,
    }
}

fn normalize_header(raw: &str) -> String {
    raw.trim().trim_matches('*').trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::{lookup, registry};
    use std::collections::{BTreeMap, BTreeSet};

    const CAPABILITIES_MD: &str =
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/CAPABILITIES.md"));

    const META_COLS: &[&str] = &["agent", "folder", "wire", "kind", "notes"];

    #[derive(Debug, Clone)]
    struct MatrixRow {
        agent: String,
        folder: Option<String>,
        wire: Option<String>,
        cells: BTreeMap<String, bool>,
    }

    #[derive(Debug)]
    struct MatrixDoc {
        columns_seen: BTreeSet<String>,
        rows: Vec<MatrixRow>,
    }

    fn split_row(line: &str) -> Vec<String> {
        let trimmed = line.trim();
        let inner = trimmed
            .strip_prefix('|')
            .unwrap_or(trimmed)
            .strip_suffix('|')
            .unwrap_or(trimmed);
        inner.split('|').map(|c| c.trim().to_string()).collect()
    }

    fn is_separator(line: &str) -> bool {
        let t = line.trim();
        t.starts_with('|')
            && t.chars().all(|c| matches!(c, '|' | ':' | '-' | ' ' | '\t'))
            && t.contains('-')
    }

    fn parse_cell(raw: &str) -> bool {
        match normalize_header(raw).as_str() {
            "" | "-" | "—" | "–" | "no" | "false" | "stub" | "n" => false,
            "✓" | "✔" | "yes" | "true" | "x" | "y" => true,
            other => panic!("unknown CAPABILITIES.md cell {other:?}"),
        }
    }

    fn parse_capabilities_md(md: &str) -> MatrixDoc {
        let mut columns_seen = BTreeSet::new();
        let mut rows_by_key: BTreeMap<String, MatrixRow> = BTreeMap::new();
        let lines: Vec<&str> = md.lines().collect();
        let mut i = 0;
        while i < lines.len() {
            let line = lines[i];
            if !line.trim_start().starts_with('|') {
                i += 1;
                continue;
            }
            let headers: Vec<String> = split_row(line)
                .into_iter()
                .map(|h| normalize_header(&h))
                .collect();
            let looks_like_matrix = headers
                .iter()
                .any(|h| h == "agent" || h == "folder" || canonical_flag_name(h).is_some());
            if !looks_like_matrix {
                i += 1;
                continue;
            }
            for h in &headers {
                if h.is_empty() || META_COLS.contains(&h.as_str()) {
                    continue;
                }
                let Some(canon) = canonical_flag_name(h) else {
                    panic!("unknown CAPABILITIES.md column {h:?}");
                };
                columns_seen.insert(canon.to_string());
            }
            i += 1;
            if i < lines.len() && is_separator(lines[i]) {
                i += 1;
            }
            while i < lines.len() && lines[i].trim_start().starts_with('|') {
                if is_separator(lines[i]) {
                    i += 1;
                    continue;
                }
                let cells = split_row(lines[i]);
                if cells.iter().all(|c| c.is_empty()) {
                    i += 1;
                    continue;
                }
                let mut row = MatrixRow {
                    agent: String::new(),
                    folder: None,
                    wire: None,
                    cells: BTreeMap::new(),
                };
                for (header, value) in headers.iter().zip(cells.iter()) {
                    match header.as_str() {
                        "agent" => row.agent = value.clone(),
                        "folder" => {
                            if !value.is_empty() {
                                row.folder = Some(value.clone());
                            }
                        }
                        "wire" => {
                            if !value.is_empty() {
                                row.wire = Some(value.clone());
                            }
                        }
                        "kind" | "notes" | "" => {}
                        _ => {
                            if let Some(canon) = canonical_flag_name(header) {
                                row.cells.insert(canon.to_string(), parse_cell(value));
                            }
                        }
                    }
                }
                if row.agent.is_empty() && row.folder.is_none() {
                    i += 1;
                    continue;
                }
                let key = row_key(&row);
                rows_by_key
                    .entry(key)
                    .and_modify(|existing| merge_row(existing, &row))
                    .or_insert(row);
                i += 1;
            }
        }
        MatrixDoc {
            columns_seen,
            rows: rows_by_key.into_values().collect(),
        }
    }

    fn row_key(row: &MatrixRow) -> String {
        if let Some(folder) = &row.folder {
            return folder.clone();
        }
        normalize_header(&row.agent).replace(' ', "_")
    }

    fn merge_row(dst: &mut MatrixRow, src: &MatrixRow) {
        if dst.folder.is_none() {
            dst.folder = src.folder.clone();
        }
        if dst.wire.is_none() {
            dst.wire = src.wire.clone();
        }
        if dst.agent.is_empty() {
            dst.agent = src.agent.clone();
        }
        for (k, v) in &src.cells {
            if let Some(prev) = dst.cells.get(k) {
                assert_eq!(
                    prev, v,
                    "CAPABILITIES.md conflict for {} flag {k}",
                    dst.agent
                );
            }
            dst.cells.insert(k.clone(), *v);
        }
    }

    fn row_matches(info: &crate::AdapterInfo, row: &MatrixRow) -> bool {
        if let Some(folder) = &row.folder {
            if folder == info.folder || folder == info.name {
                return true;
            }
        }
        let name = normalize_header(&row.agent).replace(' ', "_");
        name == info.name
            || name.contains(info.name)
            || normalize_header(&row.agent).contains(info.name)
    }

    #[test]
    fn flags_for_agent_match_associated_consts() {
        assert_eq!(
            CapabilityFlags::for_agent(AgentKind::Acp),
            CapabilityFlags::ACP
        );
        assert_eq!(
            CapabilityFlags::for_agent(AgentKind::GrokBuild),
            CapabilityFlags::GROK_BUILD
        );
        assert_eq!(CapabilityFlags::ACP, CapabilityFlags::GROK_BUILD);
    }

    #[test]
    fn registry_flags_use_for_agent() {
        for info in registry() {
            assert_eq!(
                info.flags,
                CapabilityFlags::for_agent(info.kind),
                "{}",
                info.name
            );
        }
    }

    #[test]
    fn capabilities_matrix_matches_registry() {
        let doc = parse_capabilities_md(CAPABILITIES_MD);
        let expected: BTreeSet<&str> = CapabilityFlags::CANONICAL_NAMES.iter().copied().collect();
        let seen: BTreeSet<&str> = doc.columns_seen.iter().map(String::as_str).collect();
        assert_eq!(
            seen, expected,
            "CAPABILITIES.md columns must cover every CapabilityFlags field"
        );

        assert_eq!(
            doc.rows.len(),
            registry().len(),
            "CAPABILITIES.md agent rows vs registry()"
        );

        for info in registry() {
            let row = doc
                .rows
                .iter()
                .find(|r| row_matches(info, r))
                .unwrap_or_else(|| {
                    panic!("no CAPABILITIES.md row for registry agent {}", info.name)
                });
            if let Some(wire) = &row.wire {
                assert_eq!(wire, info.wire, "{} wire", info.name);
            }
            if let Some(folder) = &row.folder {
                assert_eq!(folder, info.folder, "{} folder", info.name);
            }
            for &name in CapabilityFlags::CANONICAL_NAMES {
                let documented = row.cells.get(name).copied().unwrap_or(false);
                let actual = info.flags.get(name).expect(name);
                assert_eq!(
                    documented, actual,
                    "{} flag `{name}`: CAPABILITIES.md={documented} registry={actual}",
                    info.name
                );
            }
        }

        for row in &doc.rows {
            assert!(
                registry().iter().any(|i| row_matches(i, row)),
                "CAPABILITIES.md row {:?} has no registry entry",
                row.agent
            );
        }
    }

    #[test]
    fn matrix_parser_fails_closed_on_drift() {
        // Same tables as CAPABILITIES.md but Grok `tool` cleared — must not
        // match the live registry row.
        let drifted = r#"
| Agent | Folder | Wire | session | message | tool | thinking | plan | permission | question | compact | subagent/task |
|-------|--------|------|:-------:|:-------:|:----:|:--------:|:----:|:----------:|:--------:|:-------:|:-------------:|
| Grok Build | adapters/grok_build | jsonrpc-content-length | ✓ | ✓ | | ✓ | ✓ | ✓ | | | |

| Agent | streaming | session_persistence | session_listing | dynamic_modes | mcp_servers | rewind_conversation | rewind_files | rewind_both |
|-------|:---------:|:-------------------:|:---------------:|:-------------:|:-----------:|:-------------------:|:------------:|:-----------:|
| Grok Build | | | | | | | | |
"#;
        let doc = parse_capabilities_md(drifted);
        let grok = lookup(AgentKind::GrokBuild).unwrap();
        let row = doc
            .rows
            .iter()
            .find(|r| row_matches(grok, r))
            .expect("parsed grok row");
        assert!(
            !row.cells.get("tool_invocations").copied().unwrap_or(true),
            "fixture should document tool as false"
        );
        assert!(
            grok.flags.tool_invocations,
            "registry still has tools — drift is detectable"
        );
        assert_ne!(
            row.cells.get("tool_invocations").copied().unwrap_or(false),
            grok.flags.tool_invocations
        );
    }

    #[test]
    fn get_accepts_matrix_aliases() {
        let f = CapabilityFlags::OPENCODE;
        assert_eq!(f.get("thinking"), Some(true));
        assert_eq!(f.get("tool"), Some(true));
        assert_eq!(f.get("subagent/task"), Some(false));
        assert_eq!(f.get("not_a_flag"), None);
    }
}
