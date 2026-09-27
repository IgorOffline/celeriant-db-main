//! Identity and API keys are enforced independently.
//!
//! Identify is the mandatory first frame: any other first frame is a protocol
//! error and the connection closes, whatever the flag or keys file say.
//! `--require-client-identity` requires a verified key pair; an API key does not
//! satisfy it (10004). An `api_keys.toml` requires a valid API key whatever the
//! flag says; a key pair does not satisfy it (10005). An API key sent to a server
//! with no keys file is rejected (10006).
//!
//! Every case runs and reports; the test fails at the end listing each failed case.
//!
//! Run with: cargo run --release -p celeriant_integration_tests -- --test identity_api_key_matrix

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Duration;

use base64::Engine;
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::watch_connection::WatchOptions;
use celeriant_crypto::{Crypto, KeyPair, generate_api_key, hash_api_key};
use celeriant_msg::error_codes::{
    AUTH_INSUFFICIENT_PERMISSIONS, AUTH_INVALID_KEY, AUTH_REQUIRED, IDENTIFY_MISMATCH, IDENTIFY_REQUIRED,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::process_identify::{IDENTIFY_RESPONSE_TYPE_ID, read_identify_response, write_identify_request};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{IdentifyRequest, ReadRequest, SingleAggregateWrite, WatchRequest, WriteRequest};
use celeriant_msg::response::responses::IdentifyResponse;
use celeriant_wire::network::wire_error::WireError;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V4, WireHeader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::common::{R, event, port_for, unique_key};
use crate::{ServerConfig, TestServer};

const MAX_FRAME: u64 = 64 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);
const UNVERIFIED_CLIENT_ID: u128 = 42;

type Stream = Compat<TcpStream>;
type Res<T> = Result<T, Box<dyn Error>>;

struct Keys {
    rw: [u8; 32],
    ro: [u8; 32],
}

enum Reply {
    Identified(IdentifyResponse),
    Response(ClientResponse),
    Closed,
}

impl std::fmt::Debug for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reply::Identified(r) => write!(
                f,
                "IdentifyResponse {{ client_id: {:?}, access_level: {:?}, dict_sha: {}, dict_bytes: {:?} }}",
                r.client_id,
                r.access_level,
                r.compression_dict_sha256.is_some(),
                r.compression_dict_bytes.as_ref().map(Vec::len),
            ),
            Reply::Response(r) => write!(f, "{r:?}"),
            Reply::Closed => write!(f, "connection closed"),
        }
    }
}

pub async fn run() -> R {
    let keys = Keys { rw: generate_api_key(), ro: generate_api_key() };
    let flag = start("matrix_flag", true, None).await?;
    let flag_keys = start("matrix_flag_keys", true, Some(&keys)).await?;
    let keys_only = start("matrix_keys", false, Some(&keys)).await?;
    let plain = start("matrix_plain", false, None).await?;

    let results: Vec<(&str, R)> = vec![
        ("flag, raw Write first -> protocol error + close", write_first_refused(flag.address()).await),
        ("flag, empty Identify -> 10004", empty_identify_needs_key_pair(flag.address()).await),
        ("flag, key pair -> ok, 10003 on mismatch", key_pair_enforces_client_id(flag.address()).await),
        ("flag+keys, API key only -> 10004", api_key_does_not_satisfy_flag(flag_keys.address(), &keys).await),
        ("flag+keys, key pair only -> 10005", key_pair_does_not_satisfy_keys_file(flag_keys.address()).await),
        ("flag+keys, key pair + API key -> ok, ro blocks writes", key_pair_and_api_key_both_enforced(flag_keys.address(), &keys).await),
        ("keys, raw Write first -> protocol error + close", write_first_refused(keys_only.address()).await),
        ("keys, empty Identify -> 10005", empty_identify_needs_api_key(keys_only.address()).await),
        ("keys, API key -> any client_id writes", api_key_allows_any_client_id(keys_only.address(), &keys).await),
        ("plain, empty Identify -> dict, Write ok", empty_identify_gets_dictionary(plain.address()).await),
        ("plain, unissued API key -> 10006", unissued_api_key_refused(plain.address()).await),
        ("flag, key pair + unissued API key -> 10006", unissued_api_key_refused_with_key_pair(flag.address()).await),
        ("flag, Tokio client no identity -> IdentityRequired", client_without_identity_refused(flag.address()).await),
        ("flag+keys, Tokio client no identity -> IdentityRequired", client_without_identity_refused(flag_keys.address()).await),
        ("flag, pool.watch no identity -> IdentityRequired", pool_watch_without_identity_refused(flag.address()).await),
        ("flag+keys, pool.watch no identity -> IdentityRequired", pool_watch_without_identity_refused(flag_keys.address()).await),
        ("plain, raw Read first -> protocol error + close", read_first_refused(plain.address()).await),
    ];

    let mut failed = Vec::new();
    for (name, result) in &results {
        match result {
            Ok(()) => println!("PASS {name}"),
            Err(e) => {
                println!("FAIL {name}: {e}");
                failed.push(*name);
            }
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("{} of {} cases failed: {failed:?}", failed.len(), results.len()).into())
    }
}

async fn write_first_refused(addr: &str) -> R {
    let mut s = raw(addr).await?;
    expect_first_frame_refused(&mut s, &ClientRequest::Write(write_request(UNVERIFIED_CLIENT_ID, "write-first-refused"))).await
}

async fn empty_identify_needs_key_pair(addr: &str) -> R {
    let mut s = raw(addr).await?;
    expect_code(send_identify(&mut s, identify(None, None)?).await?, IDENTIFY_REQUIRED)
}

async fn key_pair_enforces_client_id(addr: &str) -> R {
    let mut s = raw(addr).await?;
    let pair = Crypto::generate_keypair(None)?;
    let id = verified_id(send_identify(&mut s, identify(Some(&pair), None)?).await?)?;
    expect_write_ok(send_write(&mut s, id, "key-pair-enforces-client-id").await?)?;
    expect_code(send_write(&mut s, id.wrapping_add(1), "key-pair-enforces-client-id-mismatch").await?, IDENTIFY_MISMATCH)
}

async fn api_key_does_not_satisfy_flag(addr: &str, keys: &Keys) -> R {
    let mut s = raw(addr).await?;
    expect_code(send_identify(&mut s, identify(None, Some(&keys.rw))?).await?, IDENTIFY_REQUIRED)
}

async fn key_pair_does_not_satisfy_keys_file(addr: &str) -> R {
    let mut s = raw(addr).await?;
    let pair = Crypto::generate_keypair(None)?;
    expect_code(send_identify(&mut s, identify(Some(&pair), None)?).await?, AUTH_REQUIRED)
}

async fn key_pair_and_api_key_both_enforced(addr: &str, keys: &Keys) -> R {
    let pair = Crypto::generate_keypair(None)?;

    let mut rw = raw(addr).await?;
    let id = verified_id(send_identify(&mut rw, identify(Some(&pair), Some(&keys.rw))?).await?)?;
    expect_write_ok(send_write(&mut rw, id, "key-pair-and-api-key-both-enforced-rw").await?)?;
    expect_code(send_write(&mut rw, id.wrapping_add(1), "key-pair-and-api-key-both-enforced-rw-mismatch").await?, IDENTIFY_MISMATCH)?;

    let mut ro = raw(addr).await?;
    let id = verified_id(send_identify(&mut ro, identify(Some(&pair), Some(&keys.ro))?).await?)?;
    expect_code(send_write(&mut ro, id, "key-pair-and-api-key-both-enforced-ro").await?, AUTH_INSUFFICIENT_PERMISSIONS)
}

async fn empty_identify_needs_api_key(addr: &str) -> R {
    let mut s = raw(addr).await?;
    expect_code(send_identify(&mut s, identify(None, None)?).await?, AUTH_REQUIRED)
}

async fn api_key_allows_any_client_id(addr: &str, keys: &Keys) -> R {
    let mut s = raw(addr).await?;
    expect_identified(send_identify(&mut s, identify(None, Some(&keys.rw))?).await?)?;
    expect_write_ok(send_write(&mut s, UNVERIFIED_CLIENT_ID, "api-key-allows-any-client-id-a").await?)?;
    expect_write_ok(send_write(&mut s, UNVERIFIED_CLIENT_ID + 1, "api-key-allows-any-client-id-b").await?)
}

async fn empty_identify_gets_dictionary(addr: &str) -> R {
    let mut s = raw(addr).await?;
    let resp = expect_identified(send_identify(&mut s, identify(None, None)?).await?)?;
    if resp.compression_dict_sha256.is_none() || resp.compression_dict_bytes.is_none() {
        return Err(format!("expected the dictionary in the IdentifyResponse, got {:?}", Reply::Identified(resp)).into());
    }
    expect_write_ok(send_write(&mut s, UNVERIFIED_CLIENT_ID, "empty-identify-gets-dictionary").await?)
}

async fn unissued_api_key_refused(addr: &str) -> R {
    let mut s = raw(addr).await?;
    expect_code(send_identify(&mut s, identify(None, Some(&generate_api_key()))?).await?, AUTH_INVALID_KEY)
}

async fn unissued_api_key_refused_with_key_pair(addr: &str) -> R {
    let mut s = raw(addr).await?;
    let pair = Crypto::generate_keypair(None)?;
    expect_code(send_identify(&mut s, identify(Some(&pair), Some(&generate_api_key()))?).await?, AUTH_INVALID_KEY)
}

async fn client_without_identity_refused(addr: &str) -> R {
    let mut client = CeleriantClient::connect(addr).await?;
    match client.send_request(&ClientRequest::Write(write_request(UNVERIFIED_CLIENT_ID, "client-without-identity-refused"))).await {
        Err(ClientError::IdentityRequired) => Ok(()),
        other => Err(format!("expected ClientError::IdentityRequired, got {other:?}").into()),
    }
}

async fn pool_watch_without_identity_refused(addr: &str) -> R {
    let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
    let request = WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: Some(HashSet::from([unique_key("pool-watch-without-identity-refused").org_id])),
        aggregate_types: None,
        aggregates: None,
        operation_types: None,
    };
    let options = WatchOptions { timeout: Some(DEADLINE), ..Default::default() };
    match pool.watch(request, options).await {
        Err(ClientError::IdentityRequired) => Ok(()),
        Err(other) => Err(format!("expected ClientError::IdentityRequired, got {other:?}").into()),
        Ok(_) => Err("expected ClientError::IdentityRequired, the watch connected".into()),
    }
}

async fn read_first_refused(addr: &str) -> R {
    let mut s = raw(addr).await?;
    let read = ClientRequest::Read(ReadRequest {
        correlation_id: Some(1),
        aggregate_key: unique_key("read-first-refused"),
        filters: ReadFilters::new(1),
    });
    expect_first_frame_refused(&mut s, &read).await
}

async fn start(label: &str, require_client_identity: bool, keys: Option<&Keys>) -> Res<TestServer> {
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        require_client_identity,
        insecure_allow_plaintext_auth: true,
        ..Default::default()
    };
    let dir = tempfile::TempDir::new()?;
    if let Some(keys) = keys {
        write_keys_file(dir.path(), keys)?;
    }
    TestServer::start_with_existing_dir(port_for(label), config, label.to_string(), dir).await
}

fn write_keys_file(data_root: &Path, keys: &Keys) -> std::io::Result<()> {
    let content = format!(
        "[keys]\nprimary_rw = \"{}\"\nsecondary_rw = \"{}\"\nprimary_ro = \"{}\"\nsecondary_ro = \"{}\"\n",
        hex::encode(hash_api_key(&keys.rw)),
        hex::encode(hash_api_key(&generate_api_key())),
        hex::encode(hash_api_key(&keys.ro)),
        hex::encode(hash_api_key(&generate_api_key())),
    );
    fs::write(data_root.join("api_keys.toml"), content)
}

fn identify(pair: Option<&KeyPair>, api_key: Option<&[u8; 32]>) -> Res<IdentifyRequest> {
    let (public_key, nonce, signature) = match pair {
        Some(pair) => {
            let nonce = Crypto::generate_nonce()?;
            let signature = Crypto::sign_nonce(&pair.private_key_base64, &nonce)?;
            (Some(pair.public_key_base64.clone()), Some(nonce), Some(signature))
        }
        None => (None, None, None),
    };
    Ok(IdentifyRequest {
        correlation_id: None,
        public_key,
        nonce,
        signature,
        api_key: api_key.map(|k| base64::engine::general_purpose::STANDARD.encode(k)),
        known_dict_sha256: None,
    })
}

fn write_request(client_id: u128, key_seed: &str) -> WriteRequest {
    let writes = HashMap::from([(
        unique_key(key_seed),
        SingleAggregateWrite {
            events: vec![event(1, 100, 1001, "{\"m\":1}")],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    )]);
    WriteRequest { correlation_id: Some(1), client_id, user_id: Some(888), writes }
}

async fn raw(addr: &str) -> Res<Stream> {
    Ok(TcpStream::connect(addr).await?.compat())
}

async fn send_identify(s: &mut Stream, req: IdentifyRequest) -> Res<Reply> {
    write_identify_request(s, &req, PROTOCOL_VERSION_V4).await.map_err(|e| format!("send Identify: {e:?}"))?;
    reply(s).await
}

async fn send_write(s: &mut Stream, client_id: u128, key_seed: &str) -> Res<Reply> {
    send_request(s, &ClientRequest::Write(write_request(client_id, key_seed))).await
}

async fn send_request(s: &mut Stream, req: &ClientRequest) -> Res<Reply> {
    if let Err(e) = ClientRequest::write_request(s, req, MAX_FRAME, PROTOCOL_VERSION_V4).await {
        return match e {
            WireError::NetworkError(_) => Ok(Reply::Closed),
            e => Err(format!("send request: {e:?}").into()),
        };
    }
    reply(s).await
}

async fn reply(s: &mut Stream) -> Res<Reply> {
    let header = match timeout(DEADLINE, WireHeader::from_reader(s, MAX_FRAME)).await {
        Err(_) => return Err("no reply and no close within deadline".into()),
        Ok(Err(WireError::NetworkError(_))) => return Ok(Reply::Closed),
        Ok(Err(e)) => return Err(format!("read header: {e:?}").into()),
        Ok(Ok(h)) => h,
    };
    if header.message_type == IDENTIFY_RESPONSE_TYPE_ID {
        let resp = read_identify_response(header, s).await.map_err(|e| format!("read IdentifyResponse: {e:?}"))?;
        return Ok(Reply::Identified(resp));
    }
    let resp = ClientResponse::read_from_header(header, s).await.map_err(|e| format!("read response: {e:?}"))?;
    Ok(Reply::Response(resp))
}

fn expect_code(reply: Reply, code: u32) -> R {
    match reply {
        Reply::Response(ClientResponse::GenericError(ref e)) if e.error_code == code => Ok(()),
        other => Err(format!("expected error {code}, got {other:?}").into()),
    }
}

fn expect_identified(reply: Reply) -> Res<IdentifyResponse> {
    match reply {
        Reply::Identified(resp) => Ok(resp),
        other => Err(format!("expected IdentifyResponse, got {other:?}").into()),
    }
}

fn verified_id(reply: Reply) -> Res<u128> {
    let resp = expect_identified(reply)?;
    resp.client_id.ok_or_else(|| format!("expected a verified client_id, got {:?}", Reply::Identified(resp)).into())
}

fn expect_write_ok(reply: Reply) -> R {
    match reply {
        Reply::Response(ClientResponse::Write(_)) => Ok(()),
        other => Err(format!("expected WriteResponse, got {other:?}").into()),
    }
}

/// The contract says only "protocol error", so accept a ProtocolError frame, an
/// immediate close, or a GenericError outside every existing request domain
/// (1xxx-11xxx). A domain code means the request was served or reached the
/// credential checks, not refused as a first frame.
async fn expect_first_frame_refused(s: &mut Stream, req: &ClientRequest) -> R {
    match send_request(s, req).await? {
        Reply::Closed => return Ok(()),
        Reply::Response(ClientResponse::ProtocolError(_)) => {}
        Reply::Response(ClientResponse::GenericError(ref e)) if !(1000..12000).contains(&e.error_code) => {}
        other => return Err(format!("expected a protocol error, got {other:?}").into()),
    }
    expect_closed(s).await
}

async fn expect_closed(s: &mut Stream) -> R {
    match reply(s).await? {
        Reply::Closed => Ok(()),
        other => Err(format!("expected the connection closed, got {other:?}").into()),
    }
}
