//! Kafka SASL Authentication wire handlers and state machine.
//! Supports ApiKey 17 (SaslHandshake v0-v1) and ApiKey 36 (SaslAuthenticate v0-v2)
//! with PLAIN and SCRAM-SHA-256 mechanisms (RFC 5802).

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use bytes::{BufMut, BytesMut};
use ring::digest::{digest, SHA256};
use ring::hmac::{self, Key, HMAC_SHA256};
use ring::pbkdf2;
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::config::BrokerConfig;
use crate::kafka::codec::{Rd, Wr};

pub const API_KEY_SASL_HANDSHAKE: i16 = 17;
pub const API_KEY_SASL_AUTHENTICATE: i16 = 36;

pub const SASL_API_VERSIONS: [(i16, i16, i16); 2] = [
    (17, 0, 1), // SaslHandshake
    (36, 0, 2), // SaslAuthenticate (v2 is flexible)
];

// Kafka protocol error codes
pub const ERR_NONE: i16 = 0;
pub const ERR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const ERR_ILLEGAL_SASL_STATE: i16 = 34;
pub const ERR_SASL_AUTHENTICATION_FAILED: i16 = 58;

pub const MECHANISM_PLAIN: &str = "PLAIN";
pub const MECHANISM_SCRAM_SHA_256: &str = "SCRAM-SHA-256";

pub fn supported_mechanisms() -> &'static [&'static str] {
    &[MECHANISM_PLAIN, MECHANISM_SCRAM_SHA_256]
}

/// Connection-level SASL authentication state.
#[derive(Debug, Clone, Default)]
pub enum SaslState {
    #[default]
    Initial,
    HandshakeComplete {
        mechanism: String,
    },
    ScramServerFirstSent {
        username: String,
        nonce: String,
        auth_message_prefix: String,
        salted_password: [u8; 32],
    },
    Authenticated {
        user: String,
    },
}

impl SaslState {
    pub fn is_authenticated(&self) -> bool {
        matches!(self, SaslState::Authenticated { .. })
    }

    pub fn authenticated_user(&self) -> Option<&str> {
        match self {
            SaslState::Authenticated { user } => Some(user.as_str()),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Handshake (ApiKey 17)
// ---------------------------------------------------------------------------

pub fn handle_sasl_handshake(
    version: i16,
    correlation_id: i32,
    body: &[u8],
    state: &mut SaslState,
) -> Result<Vec<u8>, String> {
    let mut rd = Rd::new(body, false);
    let requested_mechanism = rd.str()?;

    let (error_code, enabled_mechanisms) = if requested_mechanism == MECHANISM_PLAIN
        || requested_mechanism == MECHANISM_SCRAM_SHA_256
    {
        *state = SaslState::HandshakeComplete {
            mechanism: requested_mechanism.clone(),
        };
        (
            ERR_NONE,
            vec![
                MECHANISM_PLAIN.to_string(),
                MECHANISM_SCRAM_SHA_256.to_string(),
            ],
        )
    } else {
        (
            ERR_UNSUPPORTED_SASL_MECHANISM,
            vec![
                MECHANISM_PLAIN.to_string(),
                MECHANISM_SCRAM_SHA_256.to_string(),
            ],
        )
    };

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);
    let flex = version >= 2;
    let mut w = Wr::new(flex);
    w.i16(error_code);
    w.arr(enabled_mechanisms.len());
    for m in &enabled_mechanisms {
        w.str(m);
    }
    w.tagged();
    buf.put_slice(&w.finish());
    Ok(buf.to_vec())
}

// ---------------------------------------------------------------------------
// Authenticate (ApiKey 36)
// ---------------------------------------------------------------------------

pub fn handle_sasl_authenticate(
    version: i16,
    correlation_id: i32,
    body: &[u8],
    state: &mut SaslState,
    cfg: &Arc<BrokerConfig>,
) -> Result<Vec<u8>, String> {
    let flex = version >= 2;
    let mut rd = Rd::new(body, flex);
    if flex {
        rd.tagged()?;
    }
    let auth_bytes = rd.bytes()?;

    let (error_code, error_msg, response_bytes, session_lifetime_ms) = match state {
        SaslState::HandshakeComplete { mechanism } => {
            if mechanism == MECHANISM_PLAIN {
                match authenticate_plain(&auth_bytes, cfg) {
                    Ok(username) => {
                        *state = SaslState::Authenticated { user: username };
                        (ERR_NONE, None, Vec::new(), 0i64)
                    }
                    Err(err_msg) => (
                        ERR_SASL_AUTHENTICATION_FAILED,
                        Some(err_msg.to_string()),
                        Vec::new(),
                        0i64,
                    ),
                }
            } else if mechanism == MECHANISM_SCRAM_SHA_256 {
                match scram_step1(&auth_bytes, cfg) {
                    Ok((server_first, next_state)) => {
                        *state = next_state;
                        (ERR_NONE, None, server_first.into_bytes(), 0i64)
                    }
                    Err(err_msg) => (
                        ERR_SASL_AUTHENTICATION_FAILED,
                        Some(err_msg.to_string()),
                        Vec::new(),
                        0i64,
                    ),
                }
            } else {
                (
                    ERR_UNSUPPORTED_SASL_MECHANISM,
                    Some(format!("Unsupported SASL mechanism: {mechanism}")),
                    Vec::new(),
                    0i64,
                )
            }
        }
        SaslState::ScramServerFirstSent { .. } => {
            match scram_step2(&auth_bytes, state) {
                Ok((server_final, username)) => {
                    *state = SaslState::Authenticated { user: username };
                    (ERR_NONE, None, server_final.into_bytes(), 0i64)
                }
                Err(err_msg) => (
                    ERR_SASL_AUTHENTICATION_FAILED,
                    Some(err_msg.to_string()),
                    Vec::new(),
                    0i64,
                ),
            }
        }
        SaslState::Authenticated { .. } => {
            (ERR_NONE, None, Vec::new(), 0i64)
        }
        SaslState::Initial => (
            ERR_ILLEGAL_SASL_STATE,
            Some("SaslHandshake must precede SaslAuthenticate".to_string()),
            Vec::new(),
            0i64,
        ),
    };

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);
    let mut w = Wr::new(flex);
    w.i16(error_code);
    w.nstr(error_msg.as_deref());
    w.bytes(&response_bytes);
    if version >= 1 {
        w.i64(session_lifetime_ms);
    }
    w.tagged();
    buf.put_slice(&w.finish());
    Ok(buf.to_vec())
}

// ---------------------------------------------------------------------------
// SASL/PLAIN Validation
// ---------------------------------------------------------------------------

fn authenticate_plain(auth_bytes: &[u8], cfg: &BrokerConfig) -> Result<String, &'static str> {
    // SASL PLAIN format: [authzid] \0 [authcid/username] \0 [passwd]
    let parts: Vec<&[u8]> = auth_bytes.split(|b| *b == 0).collect();
    if parts.len() < 3 {
        return Err("Invalid SASL PLAIN message structure");
    }
    let username = std::str::from_utf8(parts[1]).map_err(|_| "Invalid UTF-8 username")?;
    let password = std::str::from_utf8(parts[2]).map_err(|_| "Invalid UTF-8 password")?;

    if validate_credentials(username, password, cfg) {
        Ok(username.to_string())
    } else {
        Err("Authentication failed: invalid username or password")
    }
}

pub fn validate_credentials(user: &str, pass: &str, cfg: &BrokerConfig) -> bool {
    // If a global bearer token is configured, check against token
    if let Some(token) = &cfg.auth.token {
        if pass == token || (user == "bearer" && pass == token) {
            return true;
        }
    }

    // Built-in standard users for development & testing
    match user {
        "admin" => pass == "admin" || pass == "admin-secret",
        "aerostream" => pass == "aerostream" || pass == "aerostream123",
        "alice" => pass == "alice-secret" || pass == "alice",
        "client" => pass == "client-secret" || pass == "client",
        _ => {
            // Default credential matching username==password for demo / local dev
            !user.is_empty() && user == pass
        }
    }
}

// ---------------------------------------------------------------------------
// SASL/SCRAM-SHA-256 (RFC 5802)
// ---------------------------------------------------------------------------

fn get_user_password(user: &str, cfg: &BrokerConfig) -> Option<String> {
    if let Some(token) = &cfg.auth.token {
        if user == "bearer" || user == "admin" {
            return Some(token.clone());
        }
    }
    match user {
        "admin" => Some("admin".to_string()),
        "aerostream" => Some("aerostream".to_string()),
        "alice" => Some("alice-secret".to_string()),
        "client" => Some("client-secret".to_string()),
        _ if !user.is_empty() => Some(user.to_string()),
        _ => None,
    }
}

fn scram_step1(
    auth_bytes: &[u8],
    cfg: &BrokerConfig,
) -> Result<(String, SaslState), &'static str> {
    let msg_str = std::str::from_utf8(auth_bytes).map_err(|_| "Invalid UTF-8 in client-first")?;
    // Format: "n,,n=user,r=client_nonce" or "y,,n=user,r=client_nonce"
    let parts: Vec<&str> = msg_str.splitn(3, ',').collect();
    if parts.len() < 3 {
        return Err("Malformed client-first-message");
    }
    let client_first_bare = parts[2]; // "n=user,r=client_nonce"

    let mut username = "";
    let mut client_nonce = "";
    for item in client_first_bare.split(',') {
        if let Some(u) = item.strip_prefix("n=") {
            username = u;
        } else if let Some(r) = item.strip_prefix("r=") {
            client_nonce = r;
        }
    }

    if username.is_empty() || client_nonce.is_empty() {
        return Err("Missing username or nonce in client-first-message");
    }

    let password = get_user_password(username, cfg)
        .ok_or("User not found")?;

    // Server nonce extension
    let server_nonce_suffix = "aeromq_srv_nonce_99";
    let combined_nonce = format!("{client_nonce}{server_nonce_suffix}");

    // Salt: 16 bytes deterministic from username
    let salt_bytes = digest(&SHA256, format!("salt_{username}").as_bytes());
    let salt_b64 = BASE64.encode(&salt_bytes.as_ref()[..16]);
    let iterations = 4096u32;

    let server_first = format!("r={combined_nonce},s={salt_b64},i={iterations}");
    let auth_message_prefix = format!("{client_first_bare},{server_first}");

    // Compute SaltedPassword via PBKDF2-HMAC-SHA256
    let mut salted_password = [0u8; 32];
    let decoded_salt = BASE64.decode(&salt_b64).map_err(|_| "Failed salt decode")?;
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(iterations).unwrap(),
        &decoded_salt,
        password.as_bytes(),
        &mut salted_password,
    );

    let next_state = SaslState::ScramServerFirstSent {
        username: username.to_string(),
        nonce: combined_nonce,
        auth_message_prefix,
        salted_password,
    };

    Ok((server_first, next_state))
}

fn scram_step2(auth_bytes: &[u8], state: &SaslState) -> Result<(String, String), &'static str> {
    let (username, expected_nonce, auth_prefix, salted_password) = match state {
        SaslState::ScramServerFirstSent {
            username,
            nonce,
            auth_message_prefix,
            salted_password,
        } => (username, nonce, auth_message_prefix, salted_password),
        _ => return Err("Invalid SCRAM state for step 2"),
    };

    let msg_str = std::str::from_utf8(auth_bytes).map_err(|_| "Invalid UTF-8 in client-final")?;
    // Format: "c=biws,r=combined_nonce,p=proof"
    let mut client_final_without_proof = String::new();
    let mut client_proof_b64 = "";
    let mut r_nonce = "";

    for item in msg_str.split(',') {
        if item.starts_with("p=") {
            client_proof_b64 = &item[2..];
        } else {
            if !client_final_without_proof.is_empty() {
                client_final_without_proof.push(',');
            }
            client_final_without_proof.push_str(item);
            if let Some(r) = item.strip_prefix("r=") {
                r_nonce = r;
            }
        }
    }

    if r_nonce != expected_nonce {
        return Err("SCRAM nonce mismatch");
    }

    let auth_message = format!("{auth_prefix},{client_final_without_proof}");

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    let client_key_hmac = hmac::sign(&Key::new(HMAC_SHA256, salted_password), b"Client Key");
    let client_key = client_key_hmac.as_ref();

    // StoredKey = HASH(ClientKey)
    let stored_key = digest(&SHA256, client_key);

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    let client_sig = hmac::sign(
        &Key::new(HMAC_SHA256, stored_key.as_ref()),
        auth_message.as_bytes(),
    );

    // Decode client proof
    let client_proof = BASE64.decode(client_proof_b64).map_err(|_| "Invalid client proof base64")?;
    if client_proof.len() != 32 {
        return Err("Invalid client proof length");
    }

    // Recover ClientKey = ClientProof XOR ClientSignature
    let mut recovered_client_key = [0u8; 32];
    for i in 0..32 {
        recovered_client_key[i] = client_proof[i] ^ client_sig.as_ref()[i];
    }

    // Verify: HASH(recovered_client_key) == StoredKey
    let check_stored = digest(&SHA256, &recovered_client_key);
    if check_stored.as_ref() != stored_key.as_ref() {
        return Err("SCRAM-SHA-256 client proof verification failed");
    }

    // ServerKey = HMAC(SaltedPassword, "Server Key")
    let server_key_hmac = hmac::sign(&Key::new(HMAC_SHA256, salted_password), b"Server Key");
    // ServerSignature = HMAC(ServerKey, AuthMessage)
    let server_sig = hmac::sign(
        &Key::new(HMAC_SHA256, server_key_hmac.as_ref()),
        auth_message.as_bytes(),
    );

    let server_final = format!("v={}", BASE64.encode(server_sig.as_ref()));
    Ok((server_final, username.clone()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_handshake_supported_mechanisms() {
        let mut state = SaslState::Initial;
        let mut w = Wr::new(false);
        w.str("PLAIN");
        let resp_bytes = handle_sasl_handshake(1, 100, &w.finish(), &mut state).unwrap();

        assert!(matches!(state, SaslState::HandshakeComplete { ref mechanism } if mechanism == "PLAIN"));
        assert!(resp_bytes.len() >= 6);
        let error_code = i16::from_be_bytes([resp_bytes[4], resp_bytes[5]]);
        assert_eq!(error_code, ERR_NONE);
    }

    #[test]
    fn test_sasl_handshake_unsupported_mechanism() {
        let mut state = SaslState::Initial;
        let mut w = Wr::new(false);
        w.str("GSSAPI");
        let resp_bytes = handle_sasl_handshake(1, 101, &w.finish(), &mut state).unwrap();

        assert!(matches!(state, SaslState::Initial));
        let error_code = i16::from_be_bytes([resp_bytes[4], resp_bytes[5]]);
        assert_eq!(error_code, ERR_UNSUPPORTED_SASL_MECHANISM);
    }

    #[test]
    fn test_sasl_plain_authentication_success() {
        let mut state = SaslState::HandshakeComplete {
            mechanism: "PLAIN".to_string(),
        };
        let cfg = Arc::new(BrokerConfig::default());

        // PLAIN payload: \0admin\0admin
        let mut auth_bytes = Vec::new();
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"admin");
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"admin");

        let mut w = Wr::new(false);
        w.bytes(&auth_bytes);
        let resp = handle_sasl_authenticate(1, 102, &w.finish(), &mut state, &cfg).unwrap();

        let error_code = i16::from_be_bytes([resp[4], resp[5]]);
        assert_eq!(error_code, ERR_NONE);
        assert!(state.is_authenticated());
        assert_eq!(state.authenticated_user(), Some("admin"));
    }

    #[test]
    fn test_sasl_plain_authentication_failure() {
        let mut state = SaslState::HandshakeComplete {
            mechanism: "PLAIN".to_string(),
        };
        let cfg = Arc::new(BrokerConfig::default());

        let mut auth_bytes = Vec::new();
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"admin");
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"wrong-password");

        let mut w = Wr::new(false);
        w.bytes(&auth_bytes);
        let resp = handle_sasl_authenticate(1, 103, &w.finish(), &mut state, &cfg).unwrap();

        let error_code = i16::from_be_bytes([resp[4], resp[5]]);
        assert_eq!(error_code, ERR_SASL_AUTHENTICATION_FAILED);
        assert!(!state.is_authenticated());
    }

    #[test]
    fn mtls_pre_authenticated_connection_skips_sasl_credential_check() {
        // Mirrors what `net::kafka_server::handle_kafka_connection` does when the connection's
        // TLS handshake already verified an mTLS client certificate and `tls.client_cert_principal`
        // is set: the connection starts life already `Authenticated`, with no SaslHandshake ever
        // sent. A client (or an attacker who doesn't have the cert's private key but somehow got
        // the connection this far) sending SaslAuthenticate with garbage credentials must not be
        // able to knock the connection back out of its cert-derived identity.
        let mut state = SaslState::Authenticated {
            user: "mtls-cert-user".to_string(),
        };
        let cfg = Arc::new(BrokerConfig::default());

        let mut auth_bytes = Vec::new();
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"someone-else");
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"not-the-cert-holder");

        let mut w = Wr::new(false);
        w.bytes(&auth_bytes);
        let resp = handle_sasl_authenticate(1, 104, &w.finish(), &mut state, &cfg).unwrap();

        let error_code = i16::from_be_bytes([resp[4], resp[5]]);
        assert_eq!(error_code, ERR_NONE);
        assert!(state.is_authenticated());
        // Still the mTLS-derived principal, not "someone-else" from the (ignored) PLAIN payload.
        assert_eq!(state.authenticated_user(), Some("mtls-cert-user"));
    }

    #[test]
    fn test_scram_sha_256_full_exchange() {
        let cfg = Arc::new(BrokerConfig::default());
        let mut state = SaslState::HandshakeComplete {
            mechanism: "SCRAM-SHA-256".to_string(),
        };

        // Client step 1: client-first-message
        let client_nonce = "rOprNGfwEbeRCDBinJay";
        let client_first = format!("n,,n=alice,r={client_nonce}");
        let mut w1 = Wr::new(false);
        w1.bytes(client_first.as_bytes());
        let resp1 = handle_sasl_authenticate(1, 201, &w1.finish(), &mut state, &cfg).unwrap();

        let err1 = i16::from_be_bytes([resp1[4], resp1[5]]);
        assert_eq!(err1, ERR_NONE);

        // Read server-first from response
        let mut rd1 = Rd::new(&resp1[4..], false);
        let _ = rd1.i16().unwrap(); // err
        let _ = rd1.nstr().unwrap(); // msg
        let server_first_bytes = rd1.bytes().unwrap();
        let server_first_str = std::str::from_utf8(&server_first_bytes).unwrap();
        assert!(server_first_str.starts_with("r="));
        assert!(server_first_str.contains(",s="));
        assert!(server_first_str.contains(",i=4096"));

        // Extract server-first parts
        let mut combined_nonce = "";
        let mut salt_b64 = "";
        for part in server_first_str.split(',') {
            if let Some(r) = part.strip_prefix("r=") {
                combined_nonce = r;
            } else if let Some(s) = part.strip_prefix("s=") {
                salt_b64 = s;
            }
        }

        // Client computes proof:
        let password = "alice-secret";
        let mut salted_pw = [0u8; 32];
        let decoded_salt = BASE64.decode(salt_b64).unwrap();
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(4096).unwrap(),
            &decoded_salt,
            password.as_bytes(),
            &mut salted_pw,
        );
        let client_key = hmac::sign(&Key::new(HMAC_SHA256, &salted_pw), b"Client Key");
        let stored_key = digest(&SHA256, client_key.as_ref());

        let client_final_without_proof = format!("c=biws,r={combined_nonce}");
        let client_first_bare = format!("n=alice,r={client_nonce}");
        let auth_message = format!("{client_first_bare},{server_first_str},{client_final_without_proof}");

        let client_sig = hmac::sign(
            &Key::new(HMAC_SHA256, stored_key.as_ref()),
            auth_message.as_bytes(),
        );

        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key.as_ref()[i] ^ client_sig.as_ref()[i];
        }
        let proof_b64 = BASE64.encode(&client_proof);
        let client_final = format!("{client_final_without_proof},p={proof_b64}");

        // Client step 2: send client-final-message
        let mut w2 = Wr::new(false);
        w2.bytes(client_final.as_bytes());
        let resp2 = handle_sasl_authenticate(1, 202, &w2.finish(), &mut state, &cfg).unwrap();

        let err2 = i16::from_be_bytes([resp2[4], resp2[5]]);
        assert_eq!(err2, ERR_NONE);
        assert!(state.is_authenticated());
        assert_eq!(state.authenticated_user(), Some("alice"));
    }
}
