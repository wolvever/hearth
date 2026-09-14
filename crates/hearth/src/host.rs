//! Typed runtime ticket for a host bind. Not a seventh kernel concept.
//!
//! [`HostKind`] becomes a [`Host`] (Goose, OpenCode, Grok, …) before a [`Binding`]
//! is stored. Adapter strings (`paseo-cli`, …) are [`Host::Other`].
//!
//! Known hosts share [`HostTicket`] fields. One list below drives `HostKind`,
//! `Host` variants, `as_str`, and `host()` so new kinds are one line.

use crate::{AgentId, Binding, BindingId};

/// Shared fields on every known-host ticket. Adapter strings use [`Host::Other`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostTicket {
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

macro_rules! hosts {
    ($($var:ident => $kind:literal),+ $(,)?) => {
        $(
            pub type $var = HostTicket;
        )+

        /// Known host kinds. Stored on [`Binding::kind`] as a string; not a sixth concept.
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
        pub enum HostKind {
            $($var,)+
        }

        impl HostKind {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$var => $kind,)+
                }
            }

            /// Build the typed [`Host`] ticket for this kind.
            pub fn host(self, native_resume_id: Option<String>, sandbox_id: Option<String>) -> Host {
                let ticket = HostTicket {
                    native_resume_id,
                    sandbox_id,
                };
                match self {
                    $(Self::$var => Host::$var(ticket),)+
                }
            }

            fn from_kind_str(s: &str) -> Option<Self> {
                match s {
                    $($kind => Some(Self::$var),)+
                    _ => None,
                }
            }
        }

        impl From<HostKind> for String {
            fn from(k: HostKind) -> Self {
                k.as_str().to_string()
            }
        }

        /// Typed host runtime ticket constructed from [`HostKind`] (or a raw kind string)
        /// before a [`Binding`] is recorded on the Session.
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub enum Host {
            $($var($var),)+
            /// Adapter strings (`paseo-cli`, …) that are not a HostKind variant.
            Other {
                kind: String,
                native_resume_id: Option<String>,
                sandbox_id: Option<String>,
            },
        }

        impl Host {
            /// If `kind` equals a [`HostKind::as_str`], build that variant; else [`Host::Other`].
            pub fn from_bind(
                kind: impl Into<String>,
                native_resume_id: Option<String>,
                sandbox_id: Option<String>,
            ) -> Host {
                let kind = kind.into();
                match HostKind::from_kind_str(&kind) {
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
                    $(Host::$var(_) => HostKind::$var.as_str(),)+
                    Host::Other { kind, .. } => kind,
                }
            }

            fn ticket(&self) -> Option<&HostTicket> {
                match self {
                    $(Host::$var(h) => Some(h),)+
                    Host::Other { .. } => None,
                }
            }

            pub fn native_resume_id(&self) -> Option<&str> {
                match self {
                    Host::Other {
                        native_resume_id, ..
                    } => native_resume_id.as_deref(),
                    _ => self.ticket().and_then(|t| t.native_resume_id.as_deref()),
                }
            }

            pub fn sandbox_id(&self) -> Option<&str> {
                match self {
                    Host::Other { sandbox_id, .. } => sandbox_id.as_deref(),
                    _ => self.ticket().and_then(|t| t.sandbox_id.as_deref()),
                }
            }

            /// Mint a [`Binding`]: new [`BindingId`], `kind` stays the product string.
            pub fn into_binding(self, agent: Option<AgentId>) -> Binding {
                let kind = self.kind_str().to_string();
                let (native_resume_id, sandbox_id) = match self {
                    Host::Other {
                        native_resume_id,
                        sandbox_id,
                        ..
                    } => (native_resume_id, sandbox_id),
                    $(Host::$var(h) => (h.native_resume_id, h.sandbox_id),)+
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
    };
}

hosts! {
    ClaudeCode => "claude_code",
    Codex => "codex",
    Dsh => "dsh",
    Fx => "fx",
    Pi => "pi",
    OpenCode => "opencode",
    Goose => "goose",
    Grok => "grok",
}

/// `XAI_API_KEY` or `GROK_API_KEY`. Missing or empty → `None`.
/// Test / example runner helper — not a kernel type.
pub fn grok_api_key() -> Option<String> {
    for name in ["XAI_API_KEY", "GROK_API_KEY"] {
        match std::env::var(name) {
            Ok(v) if !v.is_empty() => return Some(v),
            _ => {}
        }
    }
    None
}
