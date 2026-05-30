//! Capability tokens (L2): attenuable, offline-verifiable authority.
//!
//! A capability token is a chain of signed blocks. The first (root) block is
//! signed by the issuing agent's key; each later block is an **attenuation**
//! signed by the previous block's holder, and may only *narrow* authority. A
//! verifier needs only the token and the root issuer's public key — no network,
//! no central authority.
//!
//! Authority is expressed as **caveats**. Effective authority is the
//! *intersection* of every block's caveats, so attenuation is structurally
//! incapable of broadening: a later block listing a wider set still intersects
//! down, a later expiry still takes the earlier `min`, a later limit still takes
//! the lower `min`. Verification is **fail-closed on `action`**: a token whose
//! effective `action` set is unconstrained authorizes nothing.
//!
//! Wire format (all keys `ed25519:<hex>`, specified for re-implementation):
//! ```jsonc
//! { "_v": 1, "root_issuer": "ed25519:…",
//!   "blocks": [
//!     { "issuer": "ed25519:…", "holder": "ed25519:…",
//!       "caveats": { "action": ["task.dispatch"], "tool": ["deploy"],
//!                    "conversation": "deploy-room", "env": ["staging"],
//!                    "max_runtime_s": 60, "expires_at": "2026-05-29T13:00:00Z" },
//!       "sig": "hex(ed25519(issuer, canonical(block without sig)))" }
//!   ] }
//! ```

use crate::crypto::{self, Keypair};
use crate::error::{RaftError, Result};
use crate::util::parse_time;
use crate::util::schema_v1;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Restrictions on a block. An absent dimension is *unconstrained by this block*
/// (it inherits whatever ancestors imposed); a present dimension narrows.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct Caveats {
    /// Allow-list of action verbs (`task.dispatch`, `tool.run:deploy`, …).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) action: Vec<String>,
    /// Allow-list of tool names this token may run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) tool: Vec<String>,
    /// Single conversation this token is scoped to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conversation: Option<String>,
    /// Allow-list of execution environments (e.g. `staging`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) env: Vec<String>,
    /// Maximum task runtime, seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_runtime_s: Option<u64>,
    /// Maximum captured output, bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_output_bytes: Option<u64>,
    /// Absolute expiry (RFC3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<String>,
}

/// One link in the delegation chain.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Block {
    /// Key that signs this block (`ed25519:<hex>`).
    pub(crate) issuer: String,
    /// Key this block delegates to (`ed25519:<hex>`).
    pub(crate) holder: String,
    #[serde(default)]
    pub(crate) caveats: Caveats,
    /// Signature by `issuer` over the canonical block with `sig` removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) sig: Option<String>,
}

impl Block {
    fn signing_bytes(&self) -> Result<Vec<u8>> {
        let value = serde_json::to_value(self)?;
        crypto::canonical_omitting(&value, &["sig"])
    }
}

/// A full capability token.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Token {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    /// The key at the root of authority; equals `blocks[0].issuer`.
    pub(crate) root_issuer: String,
    pub(crate) blocks: Vec<Block>,
}

/// Issue a fresh root token from `issuer` to `holder` with `caveats`.
pub(crate) fn issue_root(issuer: &Keypair, holder: &str, caveats: Caveats) -> Result<Token> {
    let mut block = Block {
        issuer: issuer.public_hex(),
        holder: holder.to_string(),
        caveats,
        sig: None,
    };
    block.sig = Some(issuer.sign(&block.signing_bytes()?));
    Ok(Token {
        v: schema_v1(),
        root_issuer: issuer.public_hex(),
        blocks: vec![block],
    })
}

/// Append an attenuation block. `holder_key` must be the token's current holder;
/// `caveats` may only narrow (the intersection rule enforces this structurally).
pub(crate) fn attenuate(
    token: &Token,
    holder_key: &Keypair,
    new_holder: &str,
    caveats: Caveats,
) -> Result<Token> {
    let last = token
        .blocks
        .last()
        .ok_or_else(|| RaftError::coded("error", "capability token has no blocks"))?;
    if last.holder != holder_key.public_hex() {
        return Err(RaftError::coded(
            "error",
            "only the current holder of a capability may attenuate it",
        ));
    }
    let mut block = Block {
        issuer: holder_key.public_hex(),
        holder: new_holder.to_string(),
        caveats,
        sig: None,
    };
    block.sig = Some(holder_key.sign(&block.signing_bytes()?));
    let mut token = token.clone();
    token.blocks.push(block);
    Ok(token)
}

/// Resolved authority after intersecting every block. `None` set = unconstrained
/// on that dimension; an empty set would mean "nothing allowed".
pub(crate) struct Effective {
    pub(crate) action: Option<BTreeSet<String>>,
    pub(crate) tool: Option<BTreeSet<String>>,
    pub(crate) env: Option<BTreeSet<String>>,
    pub(crate) conversation: Option<String>,
    /// Two blocks pinned different conversations: authority is void.
    pub(crate) conversation_impossible: bool,
    pub(crate) max_runtime_s: Option<u64>,
    pub(crate) max_output_bytes: Option<u64>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
}

fn intersect(acc: Option<BTreeSet<String>>, next: &[String]) -> Option<BTreeSet<String>> {
    if next.is_empty() {
        return acc; // this block does not constrain the dimension
    }
    let incoming: BTreeSet<String> = next.iter().cloned().collect();
    match acc {
        None => Some(incoming),
        Some(current) => Some(current.intersection(&incoming).cloned().collect()),
    }
}

/// Compute effective authority. Parses expiries (a malformed expiry is an error,
/// never silently ignored).
pub(crate) fn effective(token: &Token) -> Result<Effective> {
    let mut action = None;
    let mut tool = None;
    let mut env = None;
    let mut conversation: Option<String> = None;
    let mut conversation_impossible = false;
    let mut max_runtime_s: Option<u64> = None;
    let mut max_output_bytes: Option<u64> = None;
    let mut expires_at: Option<DateTime<Utc>> = None;

    for block in &token.blocks {
        let caveats = &block.caveats;
        action = intersect(action, &caveats.action);
        tool = intersect(tool, &caveats.tool);
        env = intersect(env, &caveats.env);
        if let Some(conv) = &caveats.conversation {
            match &conversation {
                Some(existing) if existing != conv => conversation_impossible = true,
                _ => conversation = Some(conv.clone()),
            }
        }
        if let Some(limit) = caveats.max_runtime_s {
            max_runtime_s = Some(max_runtime_s.map_or(limit, |current| current.min(limit)));
        }
        if let Some(limit) = caveats.max_output_bytes {
            max_output_bytes = Some(max_output_bytes.map_or(limit, |current| current.min(limit)));
        }
        if let Some(raw) = &caveats.expires_at {
            let parsed = parse_time(raw)
                .map_err(|_| RaftError::coded("parse", format!("invalid expires_at {raw:?}")))?;
            expires_at = Some(expires_at.map_or(parsed, |current| current.min(parsed)));
        }
    }

    Ok(Effective {
        action,
        tool,
        env,
        conversation,
        conversation_impossible,
        max_runtime_s,
        max_output_bytes,
        expires_at,
    })
}

/// Verify every block's signature and the contiguity of the delegation chain.
/// Returns the effective authority on success. Does *not* check a request —
/// pair with [`authorize`] for that.
pub(crate) fn verify_chain(token: &Token, expected_root: Option<&str>) -> Result<Effective> {
    let first = token
        .blocks
        .first()
        .ok_or_else(|| RaftError::coded("error", "capability token has no blocks"))?;
    if first.issuer != token.root_issuer {
        return Err(RaftError::coded(
            "error",
            "first block issuer does not match root_issuer",
        ));
    }
    if let Some(root) = expected_root
        && token.root_issuer != root
    {
        return Err(RaftError::coded(
            "error",
            "capability is not rooted at the expected issuer",
        ));
    }
    let mut prev_holder: Option<&str> = None;
    for block in &token.blocks {
        if let Some(prev) = prev_holder
            && block.issuer != prev
        {
            return Err(RaftError::coded(
                "error",
                "broken delegation chain: a block is not signed by the previous holder",
            ));
        }
        let sig = block
            .sig
            .as_deref()
            .ok_or_else(|| RaftError::coded("error", "capability block is unsigned"))?;
        crypto::verify(&block.issuer, &block.signing_bytes()?, sig)?;
        prev_holder = Some(&block.holder);
    }
    effective(token)
}

/// Render effective authority as JSON for `grant inspect`/`verify`. Sets are
/// emitted as sorted arrays; `null` means unconstrained on that dimension.
pub(crate) fn effective_to_json(effective: &Effective) -> serde_json::Value {
    let set = |value: &Option<BTreeSet<String>>| match value {
        Some(items) => serde_json::Value::from(items.iter().cloned().collect::<Vec<_>>()),
        None => serde_json::Value::Null,
    };
    serde_json::json!({
        "action": set(&effective.action),
        "tool": set(&effective.tool),
        "env": set(&effective.env),
        "conversation": effective.conversation,
        "conversation_impossible": effective.conversation_impossible,
        "max_runtime_s": effective.max_runtime_s,
        "max_output_bytes": effective.max_output_bytes,
        "expires_at": effective
            .expires_at
            .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
    })
}

/// A concrete action to authorize against a token.
pub(crate) struct AuthRequest<'a> {
    pub(crate) action: &'a str,
    pub(crate) conversation: Option<&'a str>,
    pub(crate) tool: Option<&'a str>,
    pub(crate) env: Option<&'a str>,
    pub(crate) now: DateTime<Utc>,
    pub(crate) requested_runtime_s: Option<u64>,
    pub(crate) requested_output_bytes: Option<u64>,
}

/// Full authorization: verify the chain (optionally pinning the root) and check
/// the request against the effective caveats. Fail-closed: a token that does not
/// constrain `action` authorizes nothing, and a constrained dimension the
/// request omits is denied.
pub(crate) fn authorize(
    token: &Token,
    expected_root: Option<&str>,
    request: &AuthRequest,
) -> Result<()> {
    let effective = verify_chain(token, expected_root)?;
    check(&effective, request)
}

/// Check a request against already-verified effective caveats.
pub(crate) fn check(effective: &Effective, request: &AuthRequest) -> Result<()> {
    match &effective.action {
        Some(set) if set.contains(request.action) => {}
        _ => {
            return Err(RaftError::coded(
                "not_authorized",
                format!("capability does not authorize action {:?}", request.action),
            ));
        }
    }
    if effective.conversation_impossible {
        return Err(RaftError::coded(
            "not_authorized",
            "capability pins contradictory conversations",
        ));
    }
    if let Some(conversation) = &effective.conversation
        && request.conversation != Some(conversation.as_str())
    {
        return Err(RaftError::coded(
            "not_authorized",
            format!("capability is scoped to conversation {conversation:?}"),
        ));
    }
    if let Some(set) = &effective.tool {
        match request.tool {
            Some(tool) if set.contains(tool) => {}
            _ => {
                return Err(RaftError::coded(
                    "not_authorized",
                    "capability does not authorize this tool",
                ));
            }
        }
    }
    if let Some(set) = &effective.env {
        match request.env {
            Some(env) if set.contains(env) => {}
            _ => {
                return Err(RaftError::coded(
                    "not_authorized",
                    "capability does not authorize this environment",
                ));
            }
        }
    }
    if let Some(expiry) = effective.expires_at
        && request.now > expiry
    {
        return Err(RaftError::coded("not_authorized", "capability has expired"));
    }
    if let (Some(limit), Some(want)) = (effective.max_runtime_s, request.requested_runtime_s)
        && want > limit
    {
        return Err(RaftError::coded(
            "not_authorized",
            "requested runtime exceeds the capability limit",
        ));
    }
    if let (Some(limit), Some(want)) = (effective.max_output_bytes, request.requested_output_bytes)
        && want > limit
    {
        return Err(RaftError::coded(
            "not_authorized",
            "requested output size exceeds the capability limit",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::iso_now;

    fn req<'a>(action: &'a str) -> AuthRequest<'a> {
        AuthRequest {
            action,
            conversation: None,
            tool: None,
            env: None,
            now: Utc::now(),
            requested_runtime_s: None,
            requested_output_bytes: None,
        }
    }

    #[test]
    fn root_token_authorizes_its_action_and_nothing_else() {
        let alice = Keypair::generate();
        let bob = Keypair::generate();
        let token = issue_root(
            &alice,
            &bob.public_hex(),
            Caveats {
                action: vec!["task.dispatch".into()],
                ..Default::default()
            },
        )
        .unwrap();
        assert!(authorize(&token, Some(&alice.public_hex()), &req("task.dispatch")).is_ok());
        assert!(authorize(&token, Some(&alice.public_hex()), &req("task.cancel")).is_err());
        // Wrong expected root is rejected.
        assert!(authorize(&token, Some(&bob.public_hex()), &req("task.dispatch")).is_err());
    }

    #[test]
    fn unconstrained_action_authorizes_nothing() {
        let alice = Keypair::generate();
        let bob = Keypair::generate();
        let token = issue_root(&alice, &bob.public_hex(), Caveats::default()).unwrap();
        assert!(authorize(&token, None, &req("task.dispatch")).is_err());
    }

    #[test]
    fn attenuation_can_only_narrow() {
        let alice = Keypair::generate();
        let bob = Keypair::generate();
        let carol = Keypair::generate();
        let root = issue_root(
            &alice,
            &bob.public_hex(),
            Caveats {
                action: vec!["task.dispatch".into(), "task.cancel".into()],
                tool: vec!["deploy".into()],
                ..Default::default()
            },
        )
        .unwrap();
        // Bob narrows to just dispatch, and tries to ADD a tool — intersection
        // keeps only the overlap, so the broadening attempt has no effect.
        let attenuated = attenuate(
            &root,
            &bob,
            &carol.public_hex(),
            Caveats {
                action: vec!["task.dispatch".into()],
                tool: vec!["deploy".into(), "delete".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let mut request = req("task.dispatch");
        request.tool = Some("deploy");
        assert!(authorize(&attenuated, Some(&alice.public_hex()), &request).is_ok());
        // task.cancel was dropped by the attenuation.
        assert!(authorize(&attenuated, Some(&alice.public_hex()), &req("task.cancel")).is_err());
        // The "added" delete tool was never in the root, so it is not authorized.
        let mut delete = req("task.dispatch");
        delete.tool = Some("delete");
        assert!(authorize(&attenuated, Some(&alice.public_hex()), &delete).is_err());
    }

    #[test]
    fn only_the_holder_may_attenuate() {
        let alice = Keypair::generate();
        let bob = Keypair::generate();
        let mallory = Keypair::generate();
        let token = issue_root(
            &alice,
            &bob.public_hex(),
            Caveats {
                action: vec!["task.dispatch".into()],
                ..Default::default()
            },
        )
        .unwrap();
        // Mallory is not the holder (bob is), so she cannot attenuate.
        assert!(attenuate(&token, &mallory, &mallory.public_hex(), Caveats::default()).is_err());
    }

    #[test]
    fn tampering_with_a_signed_caveat_breaks_verification() {
        let alice = Keypair::generate();
        let bob = Keypair::generate();
        let mut token = issue_root(
            &alice,
            &bob.public_hex(),
            Caveats {
                action: vec!["task.dispatch".into()],
                max_runtime_s: Some(60),
                ..Default::default()
            },
        )
        .unwrap();
        // Forge a higher runtime limit without re-signing.
        token.blocks[0].caveats.max_runtime_s = Some(86_400);
        assert!(verify_chain(&token, None).is_err());
    }

    #[test]
    fn broken_chain_is_rejected() {
        let alice = Keypair::generate();
        let bob = Keypair::generate();
        let carol = Keypair::generate();
        let mut token = issue_root(
            &alice,
            &bob.public_hex(),
            Caveats {
                action: vec!["task.dispatch".into()],
                ..Default::default()
            },
        )
        .unwrap();
        // Splice a block that is NOT signed by the previous holder (bob).
        let mut rogue = Block {
            issuer: carol.public_hex(),
            holder: carol.public_hex(),
            caveats: Caveats {
                action: vec!["task.dispatch".into()],
                ..Default::default()
            },
            sig: None,
        };
        rogue.sig = Some(carol.sign(&rogue.signing_bytes().unwrap()));
        token.blocks.push(rogue);
        assert!(verify_chain(&token, None).is_err());
    }

    #[test]
    fn expiry_and_conversation_are_enforced() {
        let alice = Keypair::generate();
        let bob = Keypair::generate();
        let token = issue_root(
            &alice,
            &bob.public_hex(),
            Caveats {
                action: vec!["conversation.post".into()],
                conversation: Some("deploy-room".into()),
                expires_at: Some("2000-01-01T00:00:00Z".into()),
                ..Default::default()
            },
        )
        .unwrap();
        // Right conversation but long expired.
        let mut request = req("conversation.post");
        request.conversation = Some("deploy-room");
        assert!(authorize(&token, None, &request).is_err());
        // Wrong conversation.
        let mut other = req("conversation.post");
        other.conversation = Some("other-room");
        other.now = parse_time("2000-01-01T00:00:00Z").unwrap();
        assert!(authorize(&token, None, &other).is_err());
        // Right conversation, before expiry: ok.
        let fresh = issue_root(
            &alice,
            &bob.public_hex(),
            Caveats {
                action: vec!["conversation.post".into()],
                conversation: Some("deploy-room".into()),
                expires_at: Some(iso_now()),
                ..Default::default()
            },
        )
        .unwrap();
        let mut now_request = req("conversation.post");
        now_request.conversation = Some("deploy-room");
        // now == expiry boundary is allowed (now > expiry denies); use issue time.
        now_request.now = parse_time(&fresh.blocks[0].caveats.expires_at.clone().unwrap()).unwrap();
        assert!(authorize(&fresh, None, &now_request).is_ok());
    }
}
