use celeriant_msg::process_identify::{read_identify_request, write_identify_response, IDENTIFY_REQUEST_TYPE_ID};
use celeriant_msg::response::responses::IdentifyResponse;
use celeriant_wire::network::{wire_error::WireError, wire_header::{WireHeader, PROTOCOL_VERSION_V4}};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

pub async fn request_header<S: AsyncReadExt + AsyncWriteExt + Unpin>(stream: &mut S, max: u64) -> Result<WireHeader, WireError> {
    let header = WireHeader::from_reader(stream, max).await?;
    if header.message_type != IDENTIFY_REQUEST_TYPE_ID {
        return Ok(header);
    }
    assert_eq!(header.version, PROTOCOL_VERSION_V4);
    let request = read_identify_request(header, stream).await.unwrap();
    write_identify_response(stream, &IdentifyResponse {
        correlation_id: request.correlation_id,
        client_id: None,
        access_level: None,
        compression_dict_sha256: None,
        compression_dict_bytes: None,
    }, PROTOCOL_VERSION_V4).await.unwrap();
    WireHeader::from_reader(stream, max).await
}

pub type AdvertisedShas = std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>;

/// Answers each Identify with `dict`'s sha, shipping the bytes unless the client advertised that sha, and records every
/// advertised sha. Holds the socket until the client's next frame, then hangs up.
pub async fn dict_shipping_server(dict: &'static [u8]) -> (std::net::SocketAddr, AdvertisedShas) {
    use tokio_util::compat::TokioAsyncReadCompatExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let advertised = AdvertisedShas::default();
    let seen = advertised.clone();
    let sha = celeriant_crypto::sha256_hex(dict);
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let seen = seen.clone();
            let sha = sha.clone();
            tokio::spawn(async move {
                let mut stream = socket.compat();
                let Ok(header) = WireHeader::from_reader(&mut stream, 1 << 20).await else { return };
                let request = read_identify_request(header, &mut stream).await.unwrap();
                let ship = request.known_dict_sha256.as_deref() != Some(sha.as_str());
                seen.lock().unwrap().push(request.known_dict_sha256);
                write_identify_response(&mut stream, &IdentifyResponse {
                    correlation_id: request.correlation_id,
                    client_id: None,
                    access_level: None,
                    compression_dict_sha256: Some(sha),
                    compression_dict_bytes: ship.then(|| dict.to_vec()),
                }, PROTOCOL_VERSION_V4).await.unwrap();
                stream.flush().await.unwrap();
                let _ = WireHeader::from_reader(&mut stream, 1 << 20).await;
            });
        }
    });
    (addr, advertised)
}
