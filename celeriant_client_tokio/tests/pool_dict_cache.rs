//! The pool dict cache, bounded at the built-in plus one learned dictionary.
//! Every server is a scripted fake at one address whose connections may hold different dictionaries, as a pool sees
//! when its address list spans nodes or clusters that disagree (a misconfigured follower, a restored node, a VIP).
//!
//! Run: cargo test -p celeriant_client_tokio --test pool_dict_cache

use std::sync::{Arc, Mutex};
use std::time::Duration;

use celeriant_client_tokio::{CeleriantPool, ClientError, ClientIdentityConfig, PoolOptions, PooledConnection};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::process_identify::{read_identify_request, write_identify_response};
use celeriant_msg::request::requests::ListOrgsRequest;
use celeriant_msg::response::responses::{IdentifyResponse, ListOrgsResponse, OrgListItem};
use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V4, WireHeader};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio_util::compat::TokioAsyncReadCompatExt;

const MAX: u64 = 64 * 1024 * 1024;
const PROMPT: Duration = Duration::from_secs(5);

fn noise(seed: u64) -> Vec<u8> {
    let mut x = seed;
    (0..4096)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn sha(dict: &[u8]) -> String {
    celeriant_crypto::sha256_hex(dict)
}

fn anonymous() -> ClientIdentityConfig {
    ClientIdentityConfig { public_key: None, private_key: None, api_key: None }
}

/// What the fake does with one accepted connection, by accept order.
enum Step {
    /// Answer Identify with `dict`'s sha, shipping the bytes unless the client advertised that sha.
    Handshake(Vec<u8>),
    /// Signal `got` on receiving Identify, wait for `gate`, then answer as `Handshake`.
    HandshakeAfter(Vec<u8>, Arc<Notify>, Arc<Notify>),
    /// `Handshake`, then answer the next request with a prebuilt frame.
    HandshakeThenReply(Vec<u8>, Vec<u8>),
}

type Advertised = Arc<Mutex<Vec<Option<String>>>>;

async fn scripted(steps: Vec<Step>) -> (String, Advertised) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let advertised = Advertised::default();
    let seen = advertised.clone();
    tokio::spawn(async move {
        for step in steps {
            let Ok((socket, _)) = listener.accept().await else { return };
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut stream = socket.compat();
                let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else { return };
                let request = read_identify_request(header, &mut stream).await.unwrap();
                seen.lock().unwrap().push(request.known_dict_sha256.clone());
                let (dict, reply) = match step {
                    Step::Handshake(dict) => (dict, None),
                    Step::HandshakeAfter(dict, got, gate) => {
                        got.notify_one();
                        gate.notified().await;
                        (dict, None)
                    }
                    Step::HandshakeThenReply(dict, frame) => (dict, Some(frame)),
                };
                let dict_sha = sha(&dict);
                let ship = request.known_dict_sha256.as_deref() != Some(dict_sha.as_str());
                write_identify_response(&mut stream, &IdentifyResponse {
                    correlation_id: request.correlation_id,
                    client_id: None,
                    access_level: None,
                    compression_dict_sha256: Some(dict_sha),
                    compression_dict_bytes: ship.then_some(dict),
                }, PROTOCOL_VERSION_V4).await.unwrap();
                stream.flush().await.unwrap();
                if let Some(frame) = reply {
                    let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else { return };
                    let mut body = vec![0u8; header.compressed_length as usize];
                    stream.read_exact(&mut body).await.unwrap();
                    stream.write_all(&frame).await.unwrap();
                    stream.flush().await.unwrap();
                }
                std::future::pending::<()>().await
            });
        }
        std::future::pending::<()>().await
    });
    (addr, advertised)
}

fn pool(addr: &str, identity: Option<ClientIdentityConfig>) -> CeleriantPool {
    let options = PoolOptions::new(addr)
        .with_max_connections(4)
        .with_request_timeout(PROMPT)
        .with_connection_timeout(PROMPT);
    CeleriantPool::new(match identity {
        Some(identity) => options.with_identity(identity),
        None => options,
    })
}

/// Org ids cut from `dict`, so the compressor has to reference the dictionary and only that dictionary decodes it.
fn orgs(dict: &[u8]) -> ListOrgsResponse {
    ListOrgsResponse {
        correlation_id: Some(42),
        orgs: dict.chunks_exact(16).map(|c| OrgListItem { org_id: u128::from_le_bytes(c.try_into().unwrap()) }).collect(),
        next_cursor: None,
    }
}

async fn compressed_orgs_frame(dict: &[u8]) -> Vec<u8> {
    let codec = DictCodec::new(dict, 3).unwrap();
    let mut frame = Vec::new();
    ClientResponse::write_response(&mut frame, &ClientResponse::ListOrgs(orgs(dict)), true, &codec, MAX, PROTOCOL_VERSION_V4)
        .await
        .unwrap();
    let header = WireHeader::from_reader(&mut futures_lite::io::Cursor::new(&frame), MAX).await.unwrap();
    assert!(matches!(header.compression_type, CompressionType::ZstdDict), "the fixture must be dict-compressed");
    assert!((header.compressed_length as usize) < dict.len() / 4, "the fixture must lean on the dictionary");
    frame
}

/// A connection keeps decoding with the dictionary its own server confirmed, even after another connection has
/// replaced the pool's learned dictionary. Guards against the worst case: a response decoded with the pool's
/// "current" dictionary instead of this connection's.
#[tokio::test]
async fn a_connection_decodes_with_its_own_dict_after_the_pool_learns_another() {
    let (x, y) = (noise(1), noise(2));
    let (addr, advertised) = scripted(vec![
        Step::HandshakeThenReply(x.clone(), compressed_orgs_frame(&x).await),
        Step::Handshake(y.clone()),
    ])
    .await;
    let pool = pool(&addr, None);

    let mut a = pool.get_connection().await.unwrap();
    a.client().identify(&anonymous()).await.unwrap();
    let mut b = pool.get_connection().await.unwrap();
    b.client().identify(&anonymous()).await.unwrap();
    assert!(pool.dict_for_sha(&sha(&x)).is_none(), "setup: learning y evicted x from the pool cache");

    let request = ClientRequest::ListOrgs(ListOrgsRequest { correlation_id: Some(42), shard_id: 0, cursor: None });
    let response = tokio::time::timeout(PROMPT, a.client().send_request(&request)).await.unwrap().unwrap();

    match response {
        ClientResponse::ListOrgs(r) => {
            let got: Vec<u128> = r.orgs.iter().map(|o| o.org_id).collect();
            let want: Vec<u128> = orgs(&x).orgs.iter().map(|o| o.org_id).collect();
            let wrong = got.iter().zip(&want).filter(|(g, w)| g != w).count();
            assert!(got.len() == want.len() && wrong == 0, "decoded with the wrong dictionary: {wrong} of {} ids differ", want.len());
        }
        other => panic!("expected ListOrgs, got {other:?}"),
    }
    assert_eq!(*advertised.lock().unwrap(), [Some(sha(BUILTIN_DICT_BYTES)), Some(sha(&x))]);
}

/// Connection 1 advertises x and its server confirms x without bytes. Between the advertise and the confirm,
/// connection 2 learns y from a node holding another dictionary, and the one-slot cache evicts x. Connection 1's
/// lookup of the sha it just advertised must still hit; a miss fails the handshake with DictUnavailable although
/// the server did nothing wrong.
async fn confirmed_sha_survives_a_concurrent_learn(identity: Option<ClientIdentityConfig>) -> Result<(), ClientError> {
    let (x, y) = (noise(1), noise(2));
    let (got, gate) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (addr, advertised) = scripted(vec![
        Step::Handshake(x.clone()),
        Step::HandshakeAfter(x.clone(), got.clone(), gate.clone()),
        Step::Handshake(y.clone()),
    ])
    .await;
    let identity_pool = identity.is_some();
    let pool = pool(&addr, identity);

    let _learns_x = open(&pool, identity_pool).await.unwrap();
    let confirmed = open(&pool, identity_pool);
    let interleaved = async {
        got.notified().await;
        let conn = open(&pool, identity_pool).await.unwrap();
        gate.notify_one();
        conn
    };
    let (result, _learns_y) = tokio::time::timeout(PROMPT, async { tokio::join!(confirmed, interleaved) }).await.unwrap();

    let advertised = advertised.lock().unwrap().clone();
    assert_eq!(advertised[1], Some(sha(&x)), "setup: connection 1 advertised x");
    result.map(drop)
}

/// A pooled connection with its handshake done: an identity pool identifies while dialling, any other pool on the
/// first request, which `identify` stands in for here.
async fn open(pool: &CeleriantPool, identity_pool: bool) -> Result<PooledConnection, ClientError> {
    let mut conn = pool.get_connection().await?;
    if !identity_pool {
        conn.client().identify(&anonymous()).await?;
    }
    Ok(conn)
}

#[tokio::test]
async fn a_pool_resolves_a_sha_it_advertised_after_another_connection_learns_a_new_dict() {
    confirmed_sha_survives_a_concurrent_learn(None).await.expect("server confirmed the sha this pool advertised");
}

#[tokio::test]
async fn an_identity_pool_resolves_a_sha_it_advertised_after_another_connection_learns_a_new_dict() {
    confirmed_sha_survives_a_concurrent_learn(Some(anonymous())).await.expect("server confirmed the sha this pool advertised");
}
