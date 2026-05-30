//! Agent identity (L0) and the signed-record machinery (L1) for the raft mesh.
//!
//! An agent owns an Ed25519 keypair. The secret seed is stored locally at
//! `agents/<id>.key.json` (mode 0600) and never leaves the host. The public half
//! is published in a self-signed **passport** at `agents/<id>.passport.json` that
//! binds the human-readable agent `id` to its public key. On the mesh the public
//! key is the true identity; `id` is a convenience label, and a receiver flags an
//! `id` that arrives under an unexpected key.
//!
//! `Signed` is the generic L1 wrapper: any serializable record can be signed by a
//! keypair and verified offline against a public key, with a stable content hash
//! and an optional per-author hash-chain link.

use crate::crypto::{self, Keypair};
use crate::error::{RaftError, Result};
use crate::storage::atomic_write_json;
use crate::storage::read_json;
use crate::util::{iso_now, schema_v1};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// On-disk secret key file (mode 0600). Holds the secret seed plus the derived
/// public key and creation time for convenience.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct KeyFile {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) id: String,
    /// 32-byte Ed25519 secret seed, hex. Never transmit.
    pub(crate) secret: String,
    /// `ed25519:<hex>` public key.
    pub(crate) public: String,
    pub(crate) created_at: String,
}

/// A self-signed (optionally org-counter-signed) binding of an agent `id` to its
/// public key. This is the shareable identity document.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Passport {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) id: String,
    /// `ed25519:<hex>` public key that must verify `sig`.
    pub(crate) pubkey: String,
    #[serde(default)]
    pub(crate) capabilities: Vec<String>,
    pub(crate) issued_at: String,
    /// Self-signature over the canonical passport (this field excluded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) sig: Option<String>,
}

impl Passport {
    /// Canonical bytes the `sig` covers: the passport with `sig` removed.
    fn signing_bytes(&self) -> Result<Vec<u8>> {
        let value = serde_json::to_value(self)?;
        crypto::canonical_omitting(&value, &["sig"])
    }

    /// Verify the passport is internally consistent: `sig` is a valid signature
    /// by `pubkey` over the rest of the document.
    pub(crate) fn verify(&self) -> Result<()> {
        let sig = self
            .sig
            .as_deref()
            .ok_or_else(|| RaftError::coded("error", "passport is unsigned"))?;
        crypto::verify(&self.pubkey, &self.signing_bytes()?, sig)
    }
}

fn key_path(root: &Path, id: &str) -> PathBuf {
    root.join("agents").join(format!("{id}.key.json"))
}

fn passport_path(root: &Path, id: &str) -> PathBuf {
    root.join("agents").join(format!("{id}.passport.json"))
}

/// Create a new keypair + self-signed passport for `id`, persisting both. Errors
/// if a key already exists (identity creation must be deliberate, never silently
/// overwriting a key that other agents already trust).
pub(crate) fn create_identity(root: &Path, id: &str, capabilities: &[String]) -> Result<Passport> {
    let key_file = key_path(root, id);
    if key_file.exists() {
        return Err(RaftError::coded(
            "conflict",
            format!("identity for {id:?} already exists; refusing to overwrite its key"),
        ));
    }
    let keypair = Keypair::generate();
    let now = iso_now();
    atomic_write_json(
        &key_file,
        &KeyFile {
            v: schema_v1(),
            id: id.to_string(),
            secret: keypair.secret_hex(),
            public: keypair.public_hex(),
            created_at: now.clone(),
        },
    )?;
    let passport = sign_passport(&keypair, id, capabilities, now)?;
    atomic_write_json(&passport_path(root, id), &passport)?;
    Ok(passport)
}

/// Ensure `id` has local signing material and a valid self-signed passport,
/// creating both on first claim. Existing keys are never overwritten.
pub(crate) fn ensure_identity(root: &Path, id: &str, capabilities: &[String]) -> Result<Passport> {
    if let Some(passport) = load_passport(root, id)? {
        verify_local_identity(root, id, &passport)?;
        return Ok(passport);
    }
    if let Some(keypair) = load_keypair(root, id)? {
        let passport = sign_passport(&keypair, id, capabilities, iso_now())?;
        atomic_write_json(&passport_path(root, id), &passport)?;
        return Ok(passport);
    }
    create_identity(root, id, capabilities)
}

/// Verify that the passport and local keypair both bind to `id` and to the same
/// public key. Returns the signing keypair for records that need authentication.
pub(crate) fn load_bound_keypair(root: &Path, id: &str, expected_pubkey: &str) -> Result<Keypair> {
    let passport = load_passport(root, id)?.ok_or_else(|| {
        RaftError::coded(
            "auth_failed",
            format!("no passport for @{id}; run `raft id new {id}` or re-claim the agent"),
        )
    })?;
    verify_local_identity(root, id, &passport)?;
    if passport.pubkey != expected_pubkey {
        return Err(RaftError::coded(
            "auth_failed",
            format!("passport for @{id} no longer matches the claimed agent key"),
        ));
    }
    let keypair = load_keypair(root, id)?.ok_or_else(|| {
        RaftError::coded(
            "auth_failed",
            format!("no local keypair for @{id}; cannot sign as this agent"),
        )
    })?;
    if keypair.public_hex() != expected_pubkey {
        return Err(RaftError::coded(
            "auth_failed",
            format!("local keypair for @{id} does not match the claimed agent key"),
        ));
    }
    Ok(keypair)
}

fn verify_local_identity(root: &Path, id: &str, passport: &Passport) -> Result<()> {
    passport.verify().map_err(|err| {
        RaftError::coded(
            "auth_failed",
            format!("passport for @{id} failed verification: {}", err.message),
        )
    })?;
    if passport.id != id {
        return Err(RaftError::coded(
            "auth_failed",
            format!(
                "passport file for @{id} is bound to @{} instead",
                passport.id
            ),
        ));
    }
    let keypair = load_keypair(root, id)?.ok_or_else(|| {
        RaftError::coded(
            "auth_failed",
            format!("no local keypair for @{id}; cannot authenticate this claim"),
        )
    })?;
    if keypair.public_hex() != passport.pubkey {
        return Err(RaftError::coded(
            "auth_failed",
            format!("local keypair for @{id} does not match its passport"),
        ));
    }
    Ok(())
}

/// Build and self-sign a passport with an explicit issue time (factored out so
/// tests and re-issuance share one signing path).
fn sign_passport(
    keypair: &Keypair,
    id: &str,
    capabilities: &[String],
    issued_at: String,
) -> Result<Passport> {
    let mut passport = Passport {
        v: schema_v1(),
        id: id.to_string(),
        pubkey: keypair.public_hex(),
        capabilities: capabilities.to_vec(),
        issued_at,
        sig: None,
    };
    passport.sig = Some(keypair.sign(&passport.signing_bytes()?));
    Ok(passport)
}

/// Load an agent's keypair from disk, or `None` if it has no identity yet.
pub(crate) fn load_keypair(root: &Path, id: &str) -> Result<Option<Keypair>> {
    let Some(key_file): Option<KeyFile> = read_json(&key_path(root, id))? else {
        return Ok(None);
    };
    Ok(Some(Keypair::from_secret_hex(&key_file.secret)?))
}

/// Load an agent's passport from disk, if present.
pub(crate) fn load_passport(root: &Path, id: &str) -> Result<Option<Passport>> {
    read_json(&passport_path(root, id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passport_signs_and_verifies() {
        let keypair = Keypair::generate();
        let passport = sign_passport(&keypair, "codex", &["plan".to_string()], iso_now()).unwrap();
        assert!(passport.verify().is_ok());
        assert_eq!(passport.pubkey, keypair.public_hex());
    }

    #[test]
    fn tampered_passport_fails_verification() {
        let keypair = Keypair::generate();
        let mut passport = sign_passport(&keypair, "codex", &[], iso_now()).unwrap();
        // Forge a broader capability set without re-signing.
        passport.capabilities.push("admin".to_string());
        assert!(passport.verify().is_err());
        // Swapping the id also breaks the binding.
        let mut renamed = sign_passport(&keypair, "codex", &[], iso_now()).unwrap();
        renamed.id = "root".to_string();
        assert!(renamed.verify().is_err());
    }

    #[test]
    fn unsigned_passport_does_not_verify() {
        let keypair = Keypair::generate();
        let mut passport = sign_passport(&keypair, "a", &[], iso_now()).unwrap();
        passport.sig = None;
        assert!(passport.verify().is_err());
    }
}
