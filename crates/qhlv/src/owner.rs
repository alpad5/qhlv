//! Optional owner tokens. An app can ask for an object to be "owned" at creation;
//! the engine then generates a secret token, returns it once, and keeps only its
//! SHA-256 hash. Presenting the token later authorizes edit and delete.

use axum::http::HeaderMap;
use rand::Rng;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// Request header carrying the owner token. A header (not a URL) keeps the
/// secret out of access logs; the app builds its own manage-link around it.
pub const OWNER_TOKEN_HEADER: &str = "x-owner-token";

/// Generate a fresh 256-bit token as 64 hex characters.
pub fn new_token() -> String {
    let bytes: [u8; 32] = rand::thread_rng().gen();
    hex(&bytes)
}

/// Hash a token for storage and comparison.
pub fn hash_token(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

/// Hash of the token in the request headers; missing or non-text header is a 403.
pub fn hash_from_headers(headers: &HeaderMap) -> Result<String> {
    let token = headers
        .get(OWNER_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|t| !t.is_empty())
        .ok_or(Error::Forbidden)?;
    Ok(hash_token(token))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
