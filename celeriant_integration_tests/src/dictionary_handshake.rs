//! Dictionary handshake against a real server. The scripted-fake pool checks live in
//! `client_tokio/tests/dict_and_range_contract.rs`.
//!
//! Contract:
//! - The server answers Identify with the dictionary bytes unless `known_dict_sha256`
//!   equals its dictionary's sha, and always names that sha.
//! - `celeriant_identify_dictionary_bytes_shipped_total` on the metrics port grows by the
//!   dictionary length on every IdentifyResponse that carries the bytes. The custom
//!   dictionary here is `CUSTOM_LEN` bytes, so delta / `CUSTOM_LEN` = ships.
//! - Every Tokio client pre-seeds the built-in dictionary: a built-in server ships nothing.
//! - A pool downloads a custom dictionary at most once, across its probe connections,
//!   its data connections and `pool.watch`.
//! - A standalone `WatchConnection::connect` downloads a custom dictionary on every
//!   connect and the built-in never.
//!
//! Every server runs one shard so a watch is exactly one connection.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_crypto::{Crypto, sha256_hex};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::process_identify::{read_identify_response, write_identify_request};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{IdentifyRequest, ReadRequest, SingleAggregateWrite, WatchRequest, WriteRequest};
use celeriant_msg::response::responses::{IdentifyResponse, ReadResponse};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V4, WireHeader};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::common::{R, event, port_for, raw_identified, unique_key};
use crate::{ServerConfig, TestServer};

const SHIPPED: &str = "celeriant_identify_dictionary_bytes_shipped_total";
const CUSTOM_LEN: usize = 20_000;
const MAX_FRAME: u64 = 64 * 1024 * 1024;
const POOL_CONNECTIONS: usize = 4;

/// Deterministic noise; any bytes not opening with the zstd dictionary magic are a
/// content-only dictionary.
fn custom_dictionary() -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut out: Vec<u8> = (0..CUSTOM_LEN)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    out[0] = b'H';
    out
}

fn base_config() -> ServerConfig {
    ServerConfig { num_shards: Some(1), log_level: "warn".to_string(), standalone: true, ..Default::default() }
}

async fn start(seed: &str, config: ServerConfig, custom: bool) -> Result<TestServer, Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    if custom {
        std::fs::write(dir.path().join("dictionary.zstd_dict"), custom_dictionary())?;
    }
    TestServer::start_with_existing_dir(port_for(seed), config, seed.to_string(), dir).await
}

/// Strict scrape: a missing counter is an error, never a silent zero.
async fn shipped_bytes(server: &TestServer) -> Result<u64, Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{}/metrics", server.config().metrics_port);
    let body = reqwest::Client::builder().timeout(Duration::from_secs(2)).build()?.get(&url).send().await?.text().await?;
    let mut found = None;
    for line in body.lines().filter(|l| !l.starts_with('#')) {
        let mut parts = line.split_whitespace();
        let (Some(name), Some(value)) = (parts.next(), parts.next()) else { continue };
        let bare = name.split('{').next().unwrap_or(name);
        if bare == SHIPPED {
            *found.get_or_insert(0u64) += value.parse::<f64>()? as u64;
        }
    }
    found.ok_or_else(|| format!("{SHIPPED} is not exposed on {url}").into())
}

/// Counter delta since `before`, after letting any in-flight increment land.
async fn shipped_since(server: &TestServer, before: u64) -> Result<u64, Box<dyn std::error::Error>> {
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(shipped_bytes(server).await? - before)
}

fn expect_ships(label: &str, delta: u64, dict_len: usize, ships: u64) -> R {
    let expected = ships * dict_len as u64;
    if delta == expected {
        return Ok(());
    }
    Err(format!(
        "{label}: {SHIPPED} grew by {delta} bytes ({:.2} ships of {dict_len}), expected exactly {ships} ship(s) = {expected}",
        delta as f64 / dict_len as f64
    )
    .into())
}

async fn raw_identify(address: &str, known_sha: Option<&str>) -> Result<(Compat<TcpStream>, IdentifyResponse), String> {
    let stream = TcpStream::connect(address).await.map_err(|e| format!("connect {address}: {e}"))?;
    let _ = stream.set_nodelay(true);
    let mut stream = stream.compat();
    let request = IdentifyRequest {
        correlation_id: None,
        public_key: None,
        nonce: None,
        signature: None,
        api_key: None,
        known_dict_sha256: known_sha.map(str::to_string),
    };
    write_identify_request(&mut stream, &request, PROTOCOL_VERSION_V4).await.map_err(|e| format!("identify: {e:?}"))?;
    let header = tokio::time::timeout(Duration::from_secs(30), WireHeader::from_reader(&mut stream, MAX_FRAME))
        .await
        .map_err(|_| "identify: no response".to_string())?
        .map_err(|e| format!("identify header: {e:?}"))?;
    let response = read_identify_response(header, &mut stream).await.map_err(|e| format!("identify response: {e:?}"))?;
    Ok((stream, response))
}

/// One raw handshake row: what the client sends and whether bytes must come back.
struct Row {
    id: &'static str,
    known_sha: Option<String>,
    expect_bytes: bool,
}

/// Returns the failures for `rows` against a server holding `dict`.
async fn check_rows(server: &TestServer, dict: &[u8], rows: &[Row]) -> Vec<String> {
    let dict_sha = sha256_hex(dict);
    let mut failures = Vec::new();
    for row in rows {
        let resp = match raw_identify(server.address(), row.known_sha.as_deref()).await {
            Ok((_, resp)) => resp,
            Err(e) => {
                failures.push(format!("{}: {e}", row.id));
                continue;
            }
        };
        if resp.compression_dict_sha256.as_deref() != Some(dict_sha.as_str()) {
            failures.push(format!("{}: sha {:?}, expected {dict_sha}", row.id, resp.compression_dict_sha256));
        }
        match (&resp.compression_dict_bytes, row.expect_bytes) {
            (Some(bytes), true) if bytes.as_slice() != dict => {
                failures.push(format!("{}: shipped {} bytes that are not the server's {}-byte dictionary", row.id, bytes.len(), dict.len()))
            }
            (Some(_), true) | (None, false) => {}
            (Some(bytes), false) => failures.push(format!("{}: shipped {} dictionary bytes, expected none", row.id, bytes.len())),
            (None, true) => failures.push(format!("{}: no dictionary bytes, expected the {}-byte dictionary", row.id, dict.len())),
        }
        println!("  {}: sha {:?}, bytes {:?}", row.id, resp.compression_dict_sha256.as_deref().map(|s| &s[..12]), resp.compression_dict_bytes.as_ref().map(Vec::len));
    }
    failures
}

fn large_payload() -> String {
    format!("{{\"type\":\"order\",\"lines\":\"{}\"}}", "abcdefgh".repeat(1024))
}

/// On a connection that advertised the built-in sha and got no bytes,
/// a Read above the compression threshold arrives `ZstdDict` and decompresses with the
/// built-in dictionary to the payload written.
async fn builtin_read_decompresses(server: &TestServer) -> Result<(), String> {
    let builtin_sha = sha256_hex(BUILTIN_DICT_BYTES);
    let (mut s, resp) = raw_identify(server.address(), Some(&builtin_sha)).await?;
    if resp.compression_dict_bytes.is_some() {
        return Err("identify with the built-in sha shipped bytes".into());
    }
    let key = unique_key("dictionary_handshake_builtin_read");
    let payload = large_payload();
    let write = ClientRequest::Write(WriteRequest {
        correlation_id: Some(1),
        client_id: 7,
        user_id: None,
        writes: HashMap::from([(
            key.clone(),
            SingleAggregateWrite { events: vec![event(1, 100, 1001, &payload)], allow_create: true, expected_version: Some(0), enforce_client_idempotency: false },
        )]),
    });
    ClientRequest::write_request(&mut s, &write, MAX_FRAME, PROTOCOL_VERSION_V4).await.map_err(|e| format!("write: {e:?}"))?;
    let header = WireHeader::from_reader(&mut s, MAX_FRAME).await.map_err(|e| format!("write header: {e:?}"))?;
    match ClientResponse::read_from_header(header, &mut s).await.map_err(|e| format!("write response: {e:?}"))? {
        ClientResponse::Write(_) => {}
        other => return Err(format!("write: {other:?}")),
    }

    let read = ClientRequest::Read(ReadRequest { correlation_id: Some(2), aggregate_key: key, filters: ReadFilters::new(1) });
    ClientRequest::write_request(&mut s, &read, MAX_FRAME, PROTOCOL_VERSION_V4).await.map_err(|e| format!("read: {e:?}"))?;
    let header = WireHeader::from_reader(&mut s, MAX_FRAME).await.map_err(|e| format!("read header: {e:?}"))?;
    if header.compression_type != CompressionType::ZstdDict {
        return Err(format!("large read came back {:?}, expected ZstdDict", header.compression_type));
    }
    let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).map_err(|e| e.to_string())?;
    let resp: ReadResponse = header.read_variable_size_with_codec(&mut s, &codec).await.map_err(|e| format!("decompress with built-in: {e:?}"))?;
    let values: Vec<&[u8]> = resp.event_batches.iter().flat_map(|b| b.events.iter().map(|e| e.event_value.as_slice())).collect();
    if values != [payload.as_bytes()] {
        return Err(format!("decompressed read holds {} events, not the one {}-byte payload written", values.len(), payload.len()));
    }
    println!("  large read ZstdDict, {} -> {} bytes, payload intact", header.compressed_length, header.uncompressed_length);
    Ok(())
}

/// Which Identify gets dictionary bytes, checked on the wire, independent of the counter.
pub async fn raw_rows() -> R {
    let builtin_sha = sha256_hex(BUILTIN_DICT_BYTES);
    let custom = custom_dictionary();
    let custom_sha = sha256_hex(&custom);
    let mut failures = Vec::new();

    let builtin = start("dictionary_handshake_raw_builtin", base_config(), false).await?;
    failures.extend(
        check_rows(&builtin, BUILTIN_DICT_BYTES, &[
            Row { id: "built-in, no sha", known_sha: None, expect_bytes: true },
            Row { id: "built-in, built-in sha", known_sha: Some(builtin_sha.clone()), expect_bytes: false },
        ])
        .await,
    );
    if let Err(e) = builtin_read_decompresses(&builtin).await {
        failures.push(format!("built-in read: {e}"));
    }
    drop(builtin);

    let server = start("dictionary_handshake_raw_custom", base_config(), true).await?;
    failures.extend(
        check_rows(&server, &custom, &[
            Row { id: "custom, no sha", known_sha: None, expect_bytes: true },
            Row { id: "custom, built-in sha", known_sha: Some(builtin_sha), expect_bytes: true },
            Row { id: "custom, own sha", known_sha: Some(custom_sha), expect_bytes: false },
        ])
        .await,
    );

    if failures.is_empty() { Ok(()) } else { Err(failures.join("\n").into()) }
}

/// The counter measures what the wire shows: two raw handshakes that got bytes and
/// one that did not add exactly two dictionary lengths.
pub async fn counter_matches_wire() -> R {
    let custom = custom_dictionary();
    let server = start("dictionary_handshake_counter", base_config(), true).await?;
    let before = shipped_bytes(&server).await?;
    let mut shipped_on_wire = 0u64;
    for sha in [None, Some(sha256_hex(BUILTIN_DICT_BYTES)), Some(sha256_hex(&custom))] {
        let (_, resp) = raw_identify(server.address(), sha.as_deref()).await?;
        shipped_on_wire += resp.compression_dict_bytes.map_or(0, |b| b.len() as u64);
    }
    if shipped_on_wire != 2 * CUSTOM_LEN as u64 {
        return Err(format!("wire shipped {shipped_on_wire} bytes over three handshakes, expected {}", 2 * CUSTOM_LEN).into());
    }
    expect_ships("3 raw handshakes, 2 with bytes", shipped_since(&server, before).await?, CUSTOM_LEN, 2)
}

fn watch_request(key: &AggregateKey) -> WatchRequest {
    WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: Some(HashSet::from([key.org_id])),
        aggregate_types: None,
        aggregates: None,
        operation_types: None,
    }
}

async fn pool_write(pool: &CeleriantPool, key: &AggregateKey, seq: u64, client_id: u128) -> R {
    pool.write_events_with(key.clone(), vec![event(seq, 100, 1000 + seq, &large_payload())], client_id, WriteEventsOptions { allow_create: true, ..Default::default() })
        .await
        .map_err(|e| format!("pool write: {e:?}"))?;
    Ok(())
}

/// A built-in server, and every kind of fresh Tokio client doing real work on it,
/// large compressed payloads included. Nothing is shipped; each step's delta is named.
pub async fn builtin_never_shipped() -> R {
    let server = start("dictionary_handshake_builtin_never_shipped", base_config(), false).await?;
    let addr = server.address().to_string();
    let key = unique_key("dictionary_handshake_builtin_never_shipped");
    let mut steps: Vec<(&str, u64)> = Vec::new();

    let before = shipped_bytes(&server).await?;
    let mut client = CeleriantClient::connect(&addr).await?;
    client
        .write_events_with(key.clone(), vec![event(1, 100, 1001, &large_payload())], 0, WriteEventsOptions { allow_create: true, ..Default::default() })
        .await?;
    client.read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(1) }).await?;
    steps.push(("CeleriantClient connect, write, read", shipped_since(&server, before).await?));

    let before = shipped_bytes(&server).await?;
    let pool = CeleriantPool::new(PoolOptions::new(addr.clone()));
    pool_write(&pool, &key, 2, 0).await?;
    pool.read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(1) }).await?;
    steps.push(("fresh pool write, read", shipped_since(&server, before).await?));

    let before = shipped_bytes(&server).await?;
    let pool = CeleriantPool::new(PoolOptions::new(addr.clone()));
    let _watch = pool.watch(watch_request(&key), WatchOptions::default()).await?;
    steps.push(("fresh pool.watch", shipped_since(&server, before).await?));

    let before = shipped_bytes(&server).await?;
    let _watch = WatchConnection::connect(&addr, watch_request(&key), WatchOptions::default()).await?;
    steps.push(("standalone WatchConnection::connect", shipped_since(&server, before).await?));

    for (step, delta) in &steps {
        println!("  {step}: {delta} bytes shipped");
    }
    let shipped: Vec<String> = steps.iter().filter(|(_, d)| *d != 0).map(|(s, d)| format!("{s}: {d} bytes")).collect();
    if shipped.is_empty() {
        Ok(())
    } else {
        Err(format!("built-in server shipped its dictionary ({} bytes each): {}", BUILTIN_DICT_BYTES.len(), shipped.join("; ")).into())
    }
}

/// Holds `POOL_CONNECTIONS` pool connections at once, so each is a distinct dial, and
/// reads through every one of them.
async fn exercise_pool_connections(pool: &CeleriantPool, key: &AggregateKey, client_id: u128) -> R {
    pool_write(pool, key, 1, client_id).await?;
    let mut held = Vec::with_capacity(POOL_CONNECTIONS);
    for _ in 0..POOL_CONNECTIONS {
        let mut conn = pool.get_connection().await?;
        conn.client().read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(1) }).await?;
        held.push(conn);
    }
    let dialled = pool.stats().connections.succeeded;
    if dialled < POOL_CONNECTIONS as u64 {
        return Err(format!("harness: pool dialled {dialled} connections, needed at least {POOL_CONNECTIONS}").into());
    }
    println!("  pool dialled {dialled} connections");
    Ok(())
}

/// A pool with an identity config, several connections: one ship.
pub async fn pool_with_identity_ships_once() -> R {
    let keypair = Crypto::generate_keypair(None)?;
    let config = ServerConfig { require_client_identity: true, insecure_allow_plaintext_auth: true, ..base_config() };
    let server = start("dictionary_handshake_pool_identity", config, true).await?;
    let identity = ClientIdentityConfig::from_key_pair(keypair.public_key_base64, keypair.private_key_base64);
    // Writes must carry the id the server verified for this key; learn it before counting.
    let client_id = CeleriantClient::connect(server.address())
        .await?
        .identify(&identity)
        .await?
        .ok_or("identify under the flag returned no client id")?;
    let options = PoolOptions::new(server.address().to_string()).with_max_connections(POOL_CONNECTIONS).with_identity(identity);
    let before = shipped_bytes(&server).await?;
    exercise_pool_connections(&CeleriantPool::new(options), &unique_key("dictionary_handshake_pool_identity"), client_id).await?;
    expect_ships("pool with identity", shipped_since(&server, before).await?, CUSTOM_LEN, 1)
}

/// A pool without an identity config, several connections: one ship.
pub async fn pool_without_identity_ships_once() -> R {
    let server = start("dictionary_handshake_pool_no_identity", base_config(), true).await?;
    let options = PoolOptions::new(server.address().to_string()).with_max_connections(POOL_CONNECTIONS);
    let before = shipped_bytes(&server).await?;
    exercise_pool_connections(&CeleriantPool::new(options), &unique_key("dictionary_handshake_pool_no_identity"), 0).await?;
    expect_ships("pool without identity", shipped_since(&server, before).await?, CUSTOM_LEN, 1)
}

/// `pool.watch` first on a fresh pool, then pool writes and reads: one ship.
pub async fn pool_watch_then_write_ships_once() -> R {
    let server = start("dictionary_handshake_pool_watch", base_config(), true).await?;
    let key = unique_key("dictionary_handshake_pool_watch");
    let pool = CeleriantPool::new(PoolOptions::new(server.address().to_string()));
    let before = shipped_bytes(&server).await?;
    let mut watch = pool.watch(watch_request(&key), WatchOptions::default()).await?;
    let after_watch = shipped_since(&server, before).await?;
    pool_write(&pool, &key, 1, 0).await?;
    pool.read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(1) }).await?;
    watch
        .next_timeout(Duration::from_secs(5))
        .await
        .map_err(|e| format!("watch stream failed after the pool write: {e:?}"))?
        .ok_or("watch delivered nothing within 5s of the pool write")?;
    let total = shipped_since(&server, before).await?;
    println!("  after watch: {after_watch} bytes; after watch + write + read: {total} bytes");
    expect_ships("pool.watch then pool write", total, CUSTOM_LEN, 1)
}

/// Standalone watches have no pool cache. Custom dictionary: bytes on each of two
/// connects. Built-in: never.
pub async fn standalone_watch_connects() -> R {
    let key = unique_key("dictionary_handshake_standalone_watch");
    let mut failures = Vec::new();
    for (custom, dict_len, ships) in [(true, CUSTOM_LEN, 2), (false, BUILTIN_DICT_BYTES.len(), 0)] {
        let label = if custom { "custom, two standalone watches" } else { "built-in, two standalone watches" };
        let server = start(if custom { "dictionary_handshake_standalone_watch_custom" } else { "dictionary_handshake_standalone_watch_builtin" }, base_config(), custom).await?;
        let before = shipped_bytes(&server).await?;
        for _ in 0..2 {
            drop(WatchConnection::connect(server.address(), watch_request(&key), WatchOptions::default()).await?);
        }
        if let Err(e) = expect_ships(label, shipped_since(&server, before).await?, dict_len, ships) {
            failures.push(e.to_string());
        }
    }
    if failures.is_empty() { Ok(()) } else { Err(failures.join("\n").into()) }
}

/// Every connection holds the dictionary after Identify, so a large read is `ZstdDict`
/// whichever shard serves it, including a connection redirected to another shard.
pub async fn large_reads_on_every_shard_use_the_dictionary() -> R {
    let server = start("dictionary_handshake_every_shard", ServerConfig { num_shards: Some(4), ..base_config() }, false).await?;
    let payload = large_payload();

    let mut uncompressed = Vec::new();
    for i in 0..16 {
        let key = unique_key(&format!("dictionary-handshake-every-shard-{i}"));
        let mut s = raw_identified(server.address(), PROTOCOL_VERSION_V4).await?;
        let write = ClientRequest::Write(WriteRequest {
            correlation_id: Some(1),
            client_id: 42,
            user_id: None,
            writes: HashMap::from([(key.clone(), SingleAggregateWrite { events: vec![event(1, 100, 1001, &payload)], allow_create: true, expected_version: Some(0), enforce_client_idempotency: false })]),
        });
        ClientRequest::write_request(&mut s, &write, MAX_FRAME, PROTOCOL_VERSION_V4).await.map_err(|e| format!("{e:?}"))?;
        let header = WireHeader::from_reader(&mut s, MAX_FRAME).await.map_err(|e| format!("{e:?}"))?;
        match ClientResponse::read_from_header(header, &mut s).await.map_err(|e| format!("{e:?}"))? {
            ClientResponse::Write(_) => {}
            other => return Err(format!("write {i}: {other:?}").into()),
        }

        let mut s = raw_identified(server.address(), PROTOCOL_VERSION_V4).await?;
        let read = ClientRequest::Read(ReadRequest { correlation_id: Some(2), aggregate_key: key, filters: ReadFilters::new(1) });
        ClientRequest::write_request(&mut s, &read, MAX_FRAME, PROTOCOL_VERSION_V4).await.map_err(|e| format!("{e:?}"))?;
        let header = WireHeader::from_reader(&mut s, MAX_FRAME).await.map_err(|e| format!("{e:?}"))?;
        println!("  read {i}: compression {:?}, {} bytes on the wire", header.compression_type, header.compressed_length);
        if header.compression_type != CompressionType::ZstdDict {
            uncompressed.push(i);
        }
    }
    if uncompressed.is_empty() {
        Ok(())
    } else {
        Err(format!("{} of 16 large reads not ZstdDict on an identified connection: {uncompressed:?}", uncompressed.len()).into())
    }
}
