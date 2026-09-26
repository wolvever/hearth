//! Place-backed cross-agent memory (Codex / Claude / Hermes-shaped).
//!
//! Same class as [`crate::FakeSandbox`] / [`crate::Runtime`] — **not** a seventh
//! product name. Memory is files under a [`crate::Place`] claim, not a Queue
//! and not Session EventLog compaction.
//!
//! Do **not** conflate:
//! 1. **Session EventLog compaction** — shrinks the live transcript (`Compact`
//!    marker). It is not the memory store.
//! 2. **Place-backed memory** — durable files under a Place claim, shared
//!    across Agents/Bindings without reminting Binding and without a Queue.
//!
//! Layout (paths are Place-relative):
//! ```text
//! Place/
//!   AGENTS.md              # human instructions (always-load)
//!   memory/
//!     MEMORY.md            # curated index; inject under byte cap
//!     USER.md              # optional user profile slice (capped)
//!     topics/*.md          # detail; load on demand
//!     handoff.md           # pre-compact working state
//!   skills/*/SKILL.md      # procedural; load when relevant
//! ```
//!
//! Invariants:
//! - Inject capped index only; topics/skills on demand (`NaiveInjectFullHandbook`).
//! - Memory writes require Place claim / keep-as-claim (`NaiveWriteWithoutClaim`).
//! - PreCompactHandoff: flush working state before EventLog compact; the compact
//!   bridge *references* Place paths (`NaiveCompactWithoutHandoff`).
//! - Cross-agent = same Place + claim transfer; Session/Binding ids unchanged
//!   (no remint).
//! - Optional `applies_to` cwd so foreign projects do not leak.
//!
//! SoftExpiring + Flush-before-dispatch stay parked.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::{BindingId, Error, Event, EventId, Place, PlaceId, Result, Session};

/// Human-authored instructions. Always injected.
pub const AGENTS_MD: &str = "AGENTS.md";
/// Curated index. Always injected, byte-capped.
pub const MEMORY_MD: &str = "memory/MEMORY.md";
/// Optional user slice. Injected when present, byte-capped.
pub const USER_MD: &str = "memory/USER.md";
/// Pre-compact working state. Flush before EventLog compact.
pub const HANDOFF_MD: &str = "memory/handoff.md";

/// Inject / USER.md byte cap for the curated index (not a handbook dump).
pub const SUMMARY_BYTE_CAP: usize = 512;

/// Place-relative path for a topic file.
pub fn topic_path(name: &str) -> String {
    format!("memory/topics/{name}.md")
}

/// Place-relative path for a skill file.
pub fn skill_path(name: &str) -> String {
    format!("skills/{name}/SKILL.md")
}

/// In-memory file map for one Place. Not stored on the [`Place`] locator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryFiles {
    pub agents_md: String,
    pub memory_md: String,
    pub user_md: String,
    pub handoff_md: Option<String>,
    pub topics: HashMap<String, String>,
    pub skills: HashMap<String, String>,
}

impl Default for MemoryFiles {
    fn default() -> Self {
        Self {
            agents_md: String::new(),
            memory_md: String::new(),
            user_md: String::new(),
            handoff_md: None,
            topics: HashMap::new(),
            skills: HashMap::new(),
        }
    }
}

/// Load / write policy. `PlaceBacked` is the correct path; the `Naive*`
/// variants document holes (they must fail as specified in tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPolicy {
    /// Correct path: capped inject, claim-gated writes, PreCompactHandoff.
    PlaceBacked,
    /// HOLE: dump handbook + all topics into every turn.
    NaiveInjectFullHandbook,
    /// HOLE: compact without flushing `handoff.md`.
    NaiveCompactWithoutHandoff,
    /// HOLE: allow writes with no Place claim (split-brain).
    NaiveWriteWithoutClaim,
}

/// Always-on inject payload. Topics and skills are **not** included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectedContext {
    pub agents: String,
    pub summary: String,
    pub user: String,
    pub handoff_hint: Option<String>,
    pub bytes: usize,
}

/// Working state flushed into `memory/handoff.md` before compact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingState {
    pub objective: String,
    pub next: String,
    pub touched_files: Vec<String>,
}

struct PlaceInner {
    files: MemoryFiles,
    claim_holder: Option<BindingId>,
    applies_to_cwd: Option<String>,
}

/// Files under a Place claim. Compose with [`Place`] / [`Session`]; do not
/// treat this as a seventh kernel noun.
pub struct PlaceMemory {
    place_id: PlaceId,
    inner: Mutex<PlaceInner>,
    policy: MemoryPolicy,
}

impl PlaceMemory {
    pub fn new(place_id: PlaceId, policy: MemoryPolicy) -> Self {
        Self {
            place_id,
            inner: Mutex::new(PlaceInner {
                files: MemoryFiles::default(),
                claim_holder: None,
                applies_to_cwd: None,
            }),
            policy,
        }
    }

    /// Bind memory to an attached Place locator (id only; Place does not store files).
    pub fn for_place(place: &Place, policy: MemoryPolicy) -> Self {
        Self::new(place.id, policy)
    }

    pub fn place_id(&self) -> PlaceId {
        self.place_id
    }

    pub fn policy(&self) -> MemoryPolicy {
        self.policy
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, PlaceInner>> {
        self.inner.lock().map_err(|_| Error::Poisoned)
    }

    pub fn memory_md_snapshot(&self) -> Result<String> {
        Ok(self.lock()?.files.memory_md.clone())
    }

    pub fn files_snapshot(&self) -> Result<MemoryFiles> {
        Ok(self.lock()?.files.clone())
    }

    /// Keep-as-claim: the Binding that holds the Place may write. A second
    /// Binding is [`Error::SplitBrain`] until [`Self::transfer_claim`].
    pub fn acquire_claim(&self, binding: BindingId) -> Result<()> {
        let mut g = self.lock()?;
        if let Some(h) = g.claim_holder {
            if h != binding {
                return Err(Error::SplitBrain);
            }
            return Ok(());
        }
        g.claim_holder = Some(binding);
        Ok(())
    }

    pub fn release_claim(&self, binding: BindingId) -> Result<()> {
        let mut g = self.lock()?;
        match g.claim_holder {
            Some(h) if h == binding => {
                g.claim_holder = None;
                Ok(())
            }
            Some(_) => Err(Error::SplitBrain),
            None => Ok(()),
        }
    }

    /// Move the Place claim between Bindings. Session and Binding ids stay
    /// (no remint). Same Place.
    pub fn transfer_claim(&self, from: BindingId, to: BindingId) -> Result<()> {
        self.release_claim(from)?;
        self.acquire_claim(to)
    }

    pub fn claim_holder(&self) -> Result<Option<BindingId>> {
        Ok(self.lock()?.claim_holder)
    }

    fn require_claim(&self, binding: BindingId) -> Result<()> {
        if self.policy == MemoryPolicy::NaiveWriteWithoutClaim {
            return Ok(());
        }
        match self.claim_holder()? {
            Some(h) if h == binding => Ok(()),
            _ => Err(Error::NoPlaceClaim),
        }
    }

    pub fn set_agents_md(&self, binding: BindingId, text: impl Into<String>) -> Result<()> {
        self.require_claim(binding)?;
        self.lock()?.files.agents_md = text.into();
        Ok(())
    }

    pub fn set_memory_md(&self, binding: BindingId, text: impl Into<String>) -> Result<()> {
        self.require_claim(binding)?;
        self.lock()?.files.memory_md = text.into();
        Ok(())
    }

    pub fn set_user_md(&self, binding: BindingId, text: impl Into<String>) -> Result<()> {
        self.require_claim(binding)?;
        self.lock()?.files.user_md = text.into();
        Ok(())
    }

    pub fn put_topic(
        &self,
        binding: BindingId,
        name: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<()> {
        self.require_claim(binding)?;
        self.lock()?.files.topics.insert(name.into(), body.into());
        Ok(())
    }

    pub fn put_skill(
        &self,
        binding: BindingId,
        name: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<()> {
        self.require_claim(binding)?;
        self.lock()?.files.skills.insert(name.into(), body.into());
        Ok(())
    }

    pub fn set_applies_to_cwd(&self, binding: BindingId, cwd: Option<String>) -> Result<()> {
        self.require_claim(binding)?;
        self.lock()?.applies_to_cwd = cwd;
        Ok(())
    }

    /// Flush working state into `memory/handoff.md`. Required before compact.
    pub fn flush_handoff(&self, binding: BindingId, state: WorkingState) -> Result<()> {
        self.require_claim(binding)?;
        let body = format_handoff(&state);
        self.lock()?.files.handoff_md = Some(body);
        Ok(())
    }

    pub fn handoff(&self) -> Result<Option<String>> {
        Ok(self.lock()?.files.handoff_md.clone())
    }

    pub fn read_topic(&self, name: &str) -> Result<String> {
        self.lock()?
            .files
            .topics
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TopicMissing(name.to_string()))
    }

    pub fn read_skill(&self, name: &str) -> Result<String> {
        self.lock()?
            .files
            .skills
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TopicMissing(name.to_string()))
    }

    /// Inject index only: `AGENTS.md` + capped `MEMORY.md` + optional capped
    /// `USER.md`. Never dumps topics/skills (`NaiveInjectFullHandbook`).
    pub fn inject(&self, session_cwd: Option<&str>) -> Result<InjectedContext> {
        let g = self.lock()?;

        if let (Some(tag), Some(cwd)) = (&g.applies_to_cwd, session_cwd) {
            if tag != cwd {
                return Ok(InjectedContext {
                    agents: g.files.agents_md.clone(),
                    summary: String::new(),
                    user: String::new(),
                    handoff_hint: None,
                    bytes: g.files.agents_md.len(),
                });
            }
        }

        let agents = g.files.agents_md.clone();
        let user = if g.files.user_md.is_empty() {
            String::new()
        } else {
            truncate_summary(&g.files.user_md, SUMMARY_BYTE_CAP)
        };
        let handoff_hint = g
            .files
            .handoff_md
            .as_ref()
            .map(|_| format!("read place://{}/{}", self.place_id.0, HANDOFF_MD));

        let summary = match self.policy {
            MemoryPolicy::NaiveInjectFullHandbook => {
                let mut dump = g.files.memory_md.clone();
                for (k, v) in &g.files.topics {
                    dump.push_str(&format!("\n\n# topic:{k}\n{v}"));
                }
                dump
            }
            _ => truncate_summary(&g.files.memory_md, SUMMARY_BYTE_CAP),
        };

        let bytes = agents.len() + summary.len() + user.len();
        Ok(InjectedContext {
            agents,
            summary,
            user,
            handoff_hint,
            bytes,
        })
    }

    /// PreCompactHandoff: flush `working` into `handoff.md`, then return a
    /// compact-bridge summary that *references* Place paths (does not inline
    /// the handbook).
    pub fn compact_bridge(
        &self,
        binding: BindingId,
        working: Option<WorkingState>,
    ) -> Result<String> {
        match self.policy {
            MemoryPolicy::NaiveCompactWithoutHandoff => {
                let _ = working;
                Ok("summary: (transcript only; no place handoff)".into())
            }
            _ => {
                if let Some(w) = working {
                    self.flush_handoff(binding, w)?;
                }
                if self.handoff()?.is_none() {
                    return Err(Error::CompactLostWorkingState);
                }
                Ok(format!(
                    "summary: compacted; restore via place://{}/{} + {}",
                    self.place_id.0, HANDOFF_MD, MEMORY_MD
                ))
            }
        }
    }
}

fn format_handoff(state: &WorkingState) -> String {
    format!(
        "# handoff\nobjective: {}\nnext: {}\ntouched:\n{}\n",
        state.objective,
        state.next,
        state
            .touched_files
            .iter()
            .map(|f| format!("- {f}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn truncate_summary(s: &str, cap: usize) -> String {
    if s.len() <= cap {
        return s.to_string();
    }
    let mut end = cap;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// Cross-agent write helper. Succeeds only when `other` holds the Place claim
/// (or the `NaiveWriteWithoutClaim` hole is in effect).
pub fn cross_agent_write(
    place: &PlaceMemory,
    _holder: BindingId,
    other: BindingId,
    note: &str,
) -> Result<()> {
    place.set_memory_md(other, note)
}

/// Thin PreCompactHandoff hook used by [`Session::compact_with_handoff`].
pub(crate) fn pre_compact_handoff(
    session: &Session,
    memory: &PlaceMemory,
    binding: BindingId,
    start: EventId,
    end: EventId,
    working: Option<WorkingState>,
) -> Result<Event> {
    if session.place(memory.place_id())?.is_none() {
        return Err(Error::UnknownPlace(memory.place_id()));
    }
    let summary = memory.compact_bridge(binding, working)?;
    session.compact(start, end, summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EventBody, HostKind, InMemory, Member, PlaceAttach, PlaceProvider,
    };

    fn seeded(policy: MemoryPolicy) -> (PlaceMemory, BindingId) {
        let p = PlaceMemory::new(PlaceId::new(), policy);
        let bind = BindingId::new();
        p.acquire_claim(bind).unwrap();
        p.set_agents_md(bind, "use rustfmt").unwrap();
        p.set_memory_md(
            bind,
            &format!("{}{}", "index: deploy via make ship\n", "x".repeat(600)),
        )
        .unwrap();
        p.set_user_md(bind, "prefers short replies").unwrap();
        p.put_topic(bind, "deploy", "full deploy runbook…").unwrap();
        (p, bind)
    }

    #[test]
    fn inject_caps_summary() {
        let (p, _) = seeded(MemoryPolicy::PlaceBacked);
        let inj = p.inject(None).unwrap();
        assert!(inj.summary.len() <= SUMMARY_BYTE_CAP + 3);
        assert!(inj.agents.contains("rustfmt"));
        assert!(inj.user.contains("short replies"));
        assert!(!inj.summary.contains("topic:deploy"));
    }

    #[test]
    fn hole_naive_inject_full_handbook() {
        let (p, _) = seeded(MemoryPolicy::NaiveInjectFullHandbook);
        let inj = p.inject(None).unwrap();
        assert!(inj.summary.contains("topic:deploy"));
        assert!(inj.summary.len() > SUMMARY_BYTE_CAP);
    }

    #[test]
    fn topic_on_demand() {
        let (p, _) = seeded(MemoryPolicy::PlaceBacked);
        assert_eq!(p.read_topic("deploy").unwrap(), "full deploy runbook…");
        assert!(matches!(
            p.read_topic("missing"),
            Err(Error::TopicMissing(_))
        ));
    }

    #[test]
    fn precompact_handoff_required() {
        let (p, bind) = seeded(MemoryPolicy::PlaceBacked);
        let bridge = p
            .compact_bridge(
                bind,
                Some(WorkingState {
                    objective: "ship v2".into(),
                    next: "run integration".into(),
                    touched_files: vec!["src/main.rs".into()],
                }),
            )
            .unwrap();
        assert!(bridge.contains("handoff.md"));
        assert!(bridge.contains(&p.place_id().0.to_string()));
        assert!(p.handoff().unwrap().unwrap().contains("ship v2"));
    }

    #[test]
    fn hole_compact_without_handoff() {
        let (p, bind) = seeded(MemoryPolicy::NaiveCompactWithoutHandoff);
        let bridge = p
            .compact_bridge(
                bind,
                Some(WorkingState {
                    objective: "ship v2".into(),
                    next: "run integration".into(),
                    touched_files: vec![],
                }),
            )
            .unwrap();
        assert!(bridge.contains("no place handoff"));
        assert!(p.handoff().unwrap().is_none());
    }

    #[test]
    fn compact_refuses_without_handoff_when_no_working() {
        let (p, bind) = seeded(MemoryPolicy::PlaceBacked);
        let err = p.compact_bridge(bind, None).unwrap_err();
        assert!(matches!(err, Error::CompactLostWorkingState));
    }

    #[test]
    fn write_requires_claim() {
        let p = PlaceMemory::new(PlaceId::new(), MemoryPolicy::PlaceBacked);
        let bind = BindingId::new();
        let err = p.set_memory_md(bind, "x").unwrap_err();
        assert!(matches!(err, Error::NoPlaceClaim));
        p.acquire_claim(bind).unwrap();
        p.set_memory_md(bind, "ok").unwrap();
    }

    #[test]
    fn cross_agent_without_claim_transfer_fails() {
        let (p, bind1) = seeded(MemoryPolicy::PlaceBacked);
        let bind2 = BindingId::new();
        let err = cross_agent_write(&p, bind1, bind2, "stolen").unwrap_err();
        assert!(matches!(err, Error::NoPlaceClaim));
        p.transfer_claim(bind1, bind2).unwrap();
        cross_agent_write(&p, bind2, bind2, "owned").unwrap();
        assert_eq!(p.claim_holder().unwrap(), Some(bind2));
    }

    #[test]
    fn hole_write_without_claim_split_brain() {
        let (p, bind1) = seeded(MemoryPolicy::NaiveWriteWithoutClaim);
        let bind2 = BindingId::new();
        cross_agent_write(&p, bind1, bind2, "race").unwrap();
        assert!(p.memory_md_snapshot().unwrap().contains("race"));
    }

    #[test]
    fn cwd_scope_filters_summary() {
        let (p, bind) = seeded(MemoryPolicy::PlaceBacked);
        p.set_applies_to_cwd(bind, Some("/proj-a".into())).unwrap();
        let blank = p.inject(Some("/proj-b")).unwrap();
        assert!(blank.summary.is_empty());
        assert!(blank.user.is_empty());
        assert!(blank.agents.contains("rustfmt"));
        let ok = p.inject(Some("/proj-a")).unwrap();
        assert!(!ok.summary.is_empty());
    }

    #[test]
    fn skills_separate_from_declarative() {
        let (p, bind) = seeded(MemoryPolicy::PlaceBacked);
        p.put_skill(bind, "ship", "1. cargo release\n2. verify-prod")
            .unwrap();
        assert!(p.read_skill("ship").unwrap().contains("cargo release"));
        let inj = p.inject(None).unwrap();
        assert!(!inj.summary.contains("cargo release"));
        assert_eq!(skill_path("ship"), "skills/ship/SKILL.md");
        assert_eq!(topic_path("deploy"), "memory/topics/deploy.md");
    }

    #[test]
    fn user_md_is_capped() {
        let p = PlaceMemory::new(PlaceId::new(), MemoryPolicy::PlaceBacked);
        let bind = BindingId::new();
        p.acquire_claim(bind).unwrap();
        p.set_user_md(bind, "u".repeat(800)).unwrap();
        let inj = p.inject(None).unwrap();
        assert!(inj.user.len() <= SUMMARY_BYTE_CAP + 3);
        assert!(!inj.user.is_empty());
    }

    #[test]
    fn claim_transfer_keeps_binding_ids() {
        let store = InMemory::new();
        let a1 = store.create_agent("one", "");
        let a2 = store.create_agent("two", "");
        let session = store.create_session();
        session.join(Member::Agent(a1.id)).unwrap();
        session.join(Member::Agent(a2.id)).unwrap();
        let sid = session.id();
        let dir = std::env::temp_dir().to_string_lossy().into_owned();
        let place = session
            .attach_place(Place::local_dir(dir, PlaceAttach::MustExist))
            .unwrap();
        let b1 = session
            .bind_agent(a1.id, HostKind::Goose, None, None)
            .unwrap();
        let b2 = session
            .bind_agent(a2.id, HostKind::Codex, None, None)
            .unwrap();
        let mem = PlaceMemory::for_place(&place, MemoryPolicy::PlaceBacked);
        mem.acquire_claim(b1.id).unwrap();
        mem.set_memory_md(b1.id, "from-one").unwrap();

        mem.transfer_claim(b1.id, b2.id).unwrap();
        mem.set_memory_md(b2.id, "from-two").unwrap();

        assert_eq!(session.id(), sid);
        let binds = session.bindings().unwrap();
        assert!(binds.iter().any(|b| b.id == b1.id));
        assert!(binds.iter().any(|b| b.id == b2.id));
        assert_eq!(binds.len(), 2);
        assert_eq!(mem.claim_holder().unwrap(), Some(b2.id));
        assert!(mem.inject(None).unwrap().summary.contains("from-two"));
        assert_eq!(session.place(place.id).unwrap().unwrap().id, place.id);
    }

    #[test]
    fn compact_with_handoff_appends_place_bridge() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("scribe", "");
        let session = store.create_session();
        session.join(Member::User(user.id)).unwrap();
        session.join(Member::Agent(agent.id)).unwrap();
        let dir = std::env::temp_dir().to_string_lossy().into_owned();
        let place = session
            .attach_place(Place::local_dir(dir, PlaceAttach::MustExist))
            .unwrap();
        let bind = session
            .bind_agent(agent.id, HostKind::Goose, None, None)
            .unwrap();
        let mem = PlaceMemory::for_place(&place, MemoryPolicy::PlaceBacked);
        mem.acquire_claim(bind.id).unwrap();
        mem.set_memory_md(bind.id, "index: keep going").unwrap();

        let m1 = session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "old-a".into(),
            })
            .unwrap();
        let m2 = session
            .append(EventBody::AgentMessage {
                agent: agent.id,
                text: "old-b".into(),
            })
            .unwrap();
        session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "keep".into(),
            })
            .unwrap();

        let compact = session
            .compact_with_handoff(
                &mem,
                bind.id,
                m1.id,
                m2.id,
                Some(WorkingState {
                    objective: "ship v2".into(),
                    next: "test".into(),
                    touched_files: vec!["src/lib.rs".into()],
                }),
            )
            .unwrap();
        assert!(matches!(
            &compact.body,
            EventBody::Compact { summary, .. }
                if summary.contains("handoff.md") && summary.contains(&place.id.0.to_string())
        ));
        assert!(mem.handoff().unwrap().unwrap().contains("ship v2"));
        let surface = session.surface().unwrap();
        assert!(!surface.iter().any(|e| e.id == m1.id || e.id == m2.id));
        assert!(surface.iter().any(|e| matches!(
            &e.body,
            EventBody::Compact { summary, .. } if summary.contains(HANDOFF_MD)
        )));
    }

    #[test]
    fn compact_with_handoff_unknown_place() {
        let store = InMemory::new();
        let session = store.create_session();
        let mem = PlaceMemory::new(PlaceId::new(), MemoryPolicy::PlaceBacked);
        let bind = BindingId::new();
        mem.acquire_claim(bind).unwrap();
        let e1 = EventId::new();
        let e2 = EventId::new();
        let err = session
            .compact_with_handoff(&mem, bind, e1, e2, None)
            .unwrap_err();
        assert!(matches!(err, Error::UnknownPlace(_)));
    }

    #[test]
    fn raw_compact_without_handoff_is_the_documented_hole() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let session = store.create_session();
        let m1 = session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "old".into(),
            })
            .unwrap();
        let mem = PlaceMemory::new(PlaceId::new(), MemoryPolicy::PlaceBacked);
        let bind = BindingId::new();
        mem.acquire_claim(bind).unwrap();
        // EventLog primitive skips PreCompactHandoff — working state is not flushed.
        session.compact(m1.id, m1.id, "transcript only").unwrap();
        assert!(mem.handoff().unwrap().is_none());
    }

    #[test]
    fn place_is_not_a_seventh_noun() {
        // PlaceMemory composes with Place; provider/instance stay on the locator.
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir().to_string_lossy().into_owned();
        let place = session
            .attach_place(Place::local_dir(dir, PlaceAttach::MustExist))
            .unwrap();
        assert!(matches!(place.provider, PlaceProvider::LocalDir));
        let mem = PlaceMemory::for_place(&place, MemoryPolicy::PlaceBacked);
        assert_eq!(mem.place_id(), place.id);
        assert_eq!(AGENTS_MD, "AGENTS.md");
        assert_eq!(MEMORY_MD, "memory/MEMORY.md");
        assert_eq!(USER_MD, "memory/USER.md");
        assert_eq!(HANDOFF_MD, "memory/handoff.md");
    }
}
