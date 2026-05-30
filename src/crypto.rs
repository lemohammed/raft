//! Low-level cryptographic primitives for the raft mesh.
//!
//! Every format here is specified so other implementations can interoperate —
//! no format depends on a Rust crate's internal representation:
//!
//! - **Public keys** are `ed25519:<64 lowercase hex chars>` (32 raw bytes).
//! - **Signatures** are 128 lowercase hex chars (64 raw bytes), Ed25519 over the
//!   canonical bytes of the signed record.
//! - **Content hashes** are `sha256:<64 lowercase hex chars>`.
//! - **Canonical bytes** of a JSON value are compact UTF-8 JSON (no insignificant
//!   whitespace) with object keys sorted lexicographically by Unicode code point
//!   — the subset of RFC 8785 (JCS) that raft's data model emits (strings,
//!   integers, booleans, arrays, nested objects). raft serializes through
//!   `serde_json::Value`, whose objects are backed by `BTreeMap`, so key order is
//!   deterministic and sorted by construction.
//!
//! A signature covers `canonical(record)` with the `sig` field removed. A record
//! `hash` is `sha256(canonical(record))` with both `hash` and `sig` removed, so
//! the hash is stable independent of who later signs it and the signature binds
//! the hash (and thus any hash-chain link) into the authenticated bytes.

use crate::error::{RaftError, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub(crate) const PUBKEY_PREFIX: &str = "ed25519:";
pub(crate) const HASH_PREFIX: &str = "sha256:";

/// Lowercase-hex encode bytes.
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    out
}

/// Decode lowercase-or-uppercase hex into bytes.
pub(crate) fn hex_decode(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 {
        return Err(RaftError::coded("parse", "hex string has odd length"));
    }
    (0..value.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&value[i..i + 2], 16)
                .map_err(|_| RaftError::coded("parse", "invalid hex digit"))
        })
        .collect()
}

/// `sha256:<hex>` content hash of arbitrary bytes.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{HASH_PREFIX}{}", hex_encode(&Sha256::digest(bytes)))
}

/// Canonical bytes of any serializable value: compact JSON with object keys
/// sorted by code point (via `serde_json::Value`'s `BTreeMap` backing).
// Consumed by L1 message/receipt signing (next milestone); the passport path
// uses `canonical_omitting`.
#[allow(dead_code)]
pub(crate) fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    Ok(serde_json::to_vec(&value)?)
}

/// Canonical bytes of a value with the named top-level keys removed. Used to
/// sign over a record while excluding its own `sig` (and to hash while excluding
/// `hash`/`sig`). Non-object values are returned canonicalized unchanged.
pub(crate) fn canonical_omitting(value: &serde_json::Value, omit: &[&str]) -> Result<Vec<u8>> {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        for key in omit {
            object.remove(*key);
        }
    }
    Ok(serde_json::to_vec(&value)?)
}

/// An Ed25519 keypair. The secret half never leaves the host; only the public
/// half (and signatures) are ever shared.
pub(crate) struct Keypair {
    signing: SigningKey,
}

impl Keypair {
    /// Generate a fresh keypair from the OS CSPRNG.
    pub(crate) fn generate() -> Self {
        Self {
            signing: SigningKey::generate(&mut OsRng),
        }
    }

    /// Reconstruct from the 32-byte secret seed in hex.
    // Used via `identity::load_keypair` by capability issuance / message signing.
    #[allow(dead_code)]
    pub(crate) fn from_secret_hex(secret_hex: &str) -> Result<Self> {
        let bytes = hex_decode(secret_hex)?;
        let seed: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| RaftError::coded("parse", "secret key must be 32 bytes"))?;
        Ok(Self {
            signing: SigningKey::from_bytes(&seed),
        })
    }

    /// The 32-byte secret seed in hex (store at mode 0600; never transmit).
    pub(crate) fn secret_hex(&self) -> String {
        hex_encode(&self.signing.to_bytes())
    }

    /// The public key as `ed25519:<hex>`.
    pub(crate) fn public_hex(&self) -> String {
        format!(
            "{PUBKEY_PREFIX}{}",
            hex_encode(&self.signing.verifying_key().to_bytes())
        )
    }

    /// Detached signature over `message`, as 128 hex chars.
    pub(crate) fn sign(&self, message: &[u8]) -> String {
        hex_encode(&self.signing.sign(message).to_bytes())
    }
}

/// Parse an `ed25519:<hex>` public key.
pub(crate) fn parse_pubkey(value: &str) -> Result<VerifyingKey> {
    let hex = value
        .strip_prefix(PUBKEY_PREFIX)
        .ok_or_else(|| RaftError::coded("parse", "public key must start with `ed25519:`"))?;
    let bytes = hex_decode(hex)?;
    let array: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| RaftError::coded("parse", "public key must be 32 bytes"))?;
    VerifyingKey::from_bytes(&array)
        .map_err(|_| RaftError::coded("parse", "invalid ed25519 public key"))
}

/// Verify a detached signature. Returns `Ok(())` only if `sig_hex` is a valid
/// Ed25519 signature by `public_hex` over `message`.
pub(crate) fn verify(public_hex: &str, message: &[u8], sig_hex: &str) -> Result<()> {
    let key = parse_pubkey(public_hex)?;
    let bytes = hex_decode(sig_hex)?;
    let array: [u8; 64] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| RaftError::coded("parse", "signature must be 64 bytes"))?;
    let signature = Signature::from_bytes(&array);
    key.verify(message, &signature)
        .map_err(|_| RaftError::coded("error", "signature verification failed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrips() {
        let bytes = [0u8, 1, 15, 16, 200, 255];
        assert_eq!(hex_decode(&hex_encode(&bytes)).unwrap(), bytes);
        assert_eq!(hex_encode(&[255, 0]), "ff00");
        assert!(hex_decode("abc").is_err());
        assert!(hex_decode("zz").is_err());
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let keypair = Keypair::generate();
        let public = keypair.public_hex();
        let message = b"delegate: deploy api to staging";
        let signature = keypair.sign(message);
        assert!(verify(&public, message, &signature).is_ok());
        // Tampered message fails.
        assert!(verify(&public, b"delegate: deploy api to prod", &signature).is_err());
        // Wrong key fails.
        let other = Keypair::generate();
        assert!(verify(&other.public_hex(), message, &signature).is_err());
    }

    #[test]
    fn secret_roundtrips_and_signs_identically() {
        let keypair = Keypair::generate();
        let restored = Keypair::from_secret_hex(&keypair.secret_hex()).unwrap();
        assert_eq!(keypair.public_hex(), restored.public_hex());
        let message = b"hello mesh";
        assert!(verify(&keypair.public_hex(), message, &restored.sign(message)).is_ok());
    }

    #[test]
    fn canonical_bytes_sort_keys_and_omit() {
        let value = serde_json::json!({ "b": 1, "a": 2, "sig": "zzz" });
        assert_eq!(canonical_bytes(&value).unwrap(), br#"{"a":2,"b":1,"sig":"zzz"}"#);
        assert_eq!(
            canonical_omitting(&value, &["sig"]).unwrap(),
            br#"{"a":2,"b":1}"#
        );
    }

    #[test]
    fn sha256_is_stable_and_prefixed() {
        let hash = sha256_hex(b"raft");
        assert!(hash.starts_with("sha256:"));
        assert_eq!(hash, sha256_hex(b"raft"));
        assert_ne!(hash, sha256_hex(b"raft2"));
    }
}
