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
