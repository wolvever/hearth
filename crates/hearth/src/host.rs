//! Typed runtime ticket for a host bind. Not a seventh kernel concept.
//!
//! [`HostKind`] becomes a [`Host`] (Goose, OpenCode, …) before a [`Binding`]
//! is stored. Adapter strings (`paseo-cli`, …) are [`Host::Other`].

use crate::{AgentId, Binding, BindingId, HostKind};

/// Claude Code host ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeCode {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

/// Codex host ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Codex {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

/// DeepSeek Harness host ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dsh {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

/// Fx host ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fx {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

/// Pi host ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pi {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

/// OpenCode host ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenCode {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

/// Goose host ticket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Goose {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

/// Typed host runtime ticket constructed from [`HostKind`] (or a raw kind string)
/// before a [`Binding`] is recorded on the Session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Host {
    ClaudeCode(ClaudeCode),
    Codex(Codex),
    Dsh(Dsh),
    Fx(Fx),
    Pi(Pi),
    OpenCode(OpenCode),
    Goose(Goose),
    /// Adapter strings (`paseo-cli`, …) that are not a HostKind variant.
    Other {
        kind: String,
        native_resume_id: Option<String>,
        sandbox_id: Option<String>,
    },
}

impl HostKind {
    /// Build the typed [`Host`] ticket for this kind.
    pub fn host(self, native_resume_id: Option<String>, sandbox_id: Option<String>) -> Host {
        match self {
            Self::ClaudeCode => Host::ClaudeCode(ClaudeCode {
                native_resume_id,
                sandbox_id,
            }),
            Self::Codex => Host::Codex(Codex {
                native_resume_id,
                sandbox_id,
            }),
            Self::Dsh => Host::Dsh(Dsh {
                native_resume_id,
                sandbox_id,
            }),
            Self::Fx => Host::Fx(Fx {
                native_resume_id,
                sandbox_id,
            }),
            Self::Pi => Host::Pi(Pi {
                native_resume_id,
                sandbox_id,
            }),
            Self::OpenCode => Host::OpenCode(OpenCode {
                native_resume_id,
                sandbox_id,
            }),
            Self::Goose => Host::Goose(Goose {
                native_resume_id,
                sandbox_id,
            }),
        }
    }
}

fn host_kind_from_str(s: &str) -> Option<HostKind> {
    [
        HostKind::ClaudeCode,
        HostKind::Codex,
        HostKind::Dsh,
        HostKind::Fx,
        HostKind::Pi,
        HostKind::OpenCode,
        HostKind::Goose,
    ]
    .into_iter()
    .find(|k| k.as_str() == s)
}

impl Host {
    /// If `kind` equals a [`HostKind::as_str`], build that variant; else [`Host::Other`].
    pub fn from_bind(
        kind: impl Into<String>,
        native_resume_id: Option<String>,
        sandbox_id: Option<String>,
    ) -> Host {
        let kind = kind.into();
        match host_kind_from_str(&kind) {
            Some(k) => k.host(native_resume_id, sandbox_id),
            None => Host::Other {
                kind,
                native_resume_id,
                sandbox_id,
            },
        }
    }

    pub fn kind_str(&self) -> &str {
        match self {
            Host::ClaudeCode(_) => HostKind::ClaudeCode.as_str(),
            Host::Codex(_) => HostKind::Codex.as_str(),
            Host::Dsh(_) => HostKind::Dsh.as_str(),
            Host::Fx(_) => HostKind::Fx.as_str(),
            Host::Pi(_) => HostKind::Pi.as_str(),
            Host::OpenCode(_) => HostKind::OpenCode.as_str(),
            Host::Goose(_) => HostKind::Goose.as_str(),
            Host::Other { kind, .. } => kind,
        }
    }

    pub fn native_resume_id(&self) -> Option<&str> {
        match self {
            Host::ClaudeCode(h) => h.native_resume_id.as_deref(),
            Host::Codex(h) => h.native_resume_id.as_deref(),
            Host::Dsh(h) => h.native_resume_id.as_deref(),
            Host::Fx(h) => h.native_resume_id.as_deref(),
            Host::Pi(h) => h.native_resume_id.as_deref(),
            Host::OpenCode(h) => h.native_resume_id.as_deref(),
            Host::Goose(h) => h.native_resume_id.as_deref(),
            Host::Other {
                native_resume_id, ..
            } => native_resume_id.as_deref(),
        }
    }

    pub fn sandbox_id(&self) -> Option<&str> {
        match self {
            Host::ClaudeCode(h) => h.sandbox_id.as_deref(),
            Host::Codex(h) => h.sandbox_id.as_deref(),
            Host::Dsh(h) => h.sandbox_id.as_deref(),
            Host::Fx(h) => h.sandbox_id.as_deref(),
            Host::Pi(h) => h.sandbox_id.as_deref(),
            Host::OpenCode(h) => h.sandbox_id.as_deref(),
            Host::Goose(h) => h.sandbox_id.as_deref(),
            Host::Other { sandbox_id, .. } => sandbox_id.as_deref(),
        }
    }

    /// Mint a [`Binding`]: new [`BindingId`], `kind` stays the product string.
    pub fn into_binding(self, agent: Option<AgentId>) -> Binding {
        let kind = self.kind_str().to_string();
        let (native_resume_id, sandbox_id) = match self {
            Host::ClaudeCode(h) => (h.native_resume_id, h.sandbox_id),
            Host::Codex(h) => (h.native_resume_id, h.sandbox_id),
            Host::Dsh(h) => (h.native_resume_id, h.sandbox_id),
            Host::Fx(h) => (h.native_resume_id, h.sandbox_id),
            Host::Pi(h) => (h.native_resume_id, h.sandbox_id),
            Host::OpenCode(h) => (h.native_resume_id, h.sandbox_id),
            Host::Goose(h) => (h.native_resume_id, h.sandbox_id),
            Host::Other {
                native_resume_id,
                sandbox_id,
                ..
            } => (native_resume_id, sandbox_id),
        };
        Binding {
            id: BindingId::new(),
            kind,
            native_resume_id,
            sandbox_id,
            agent,
        }
    }
}
