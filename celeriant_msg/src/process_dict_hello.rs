use bincode::{Decode, Encode};
use celeriant_wire::codec::compression::Sha256Hex;
use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{PROTOCOL_VERSION_V4, WireHeader, wire_header_write_fixed_size},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};

use crate::read_wire_data_error::ReadWireDataError;

pub const DICT_HELLO_REQUEST_TYPE_ID: u32 = 108;
pub const DICT_HELLO_RESPONSE_TYPE_ID: u32 = 109;

/// V4 bincode writes a `[u8; 32]` as its 32 bytes, with no length prefix.
const DICT_HELLO_BODY_BYTES: u32 = 32;

/// The first frame in each direction on a replication connection, always uncompressed.
/// The dialer sends its dictionary sha and the acceptor answers with its own, so both
/// sides know the pair before any `ZstdDict` frame can cross.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct DictHello {
    pub dict_sha256: [u8; 32],
}

/// The two nodes compress with different dictionaries. A config error: never retried into success.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DictMismatch {
    pub local_sha256: [u8; 32],
    pub peer_sha256: [u8; 32],
}

/// Hex, so the shas in any `{:?}` log line match `server_meta.toml`.
impl std::fmt::Debug for DictMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DictMismatch {{ local_sha256: {}, peer_sha256: {} }}", Sha256Hex(self.local_sha256), Sha256Hex(self.peer_sha256))
    }
}

impl std::fmt::Display for DictMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "internode dictionary mismatch: local sha {}, peer sha {}",
            Sha256Hex(self.local_sha256),
            Sha256Hex(self.peer_sha256)
        )
    }
}

/// Compared by sha, never by name: two trained dictionaries can share the default name.
pub fn check_dict_agreement(local_sha256: &[u8; 32], peer: &DictHello) -> Result<(), DictMismatch> {
    if *local_sha256 == peer.dict_sha256 {
        Ok(())
    } else {
        Err(DictMismatch { local_sha256: *local_sha256, peer_sha256: peer.dict_sha256 })
    }
}

pub async fn write_dict_hello<W>(writer: &mut W, hello: &DictHello, message_type: u32) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    wire_header_write_fixed_size(writer, hello, message_type, PROTOCOL_VERSION_V4).await
}

/// Reads exactly one hello frame of `message_type`. The header is bounded to the hello body,
/// so a peer that opens with anything else costs at most a header and a stack buffer.
pub async fn read_dict_hello<R>(reader: &mut R, message_type: u32) -> Result<DictHello, ReadWireDataError>
where
    R: AsyncReadExt + Unpin,
{
    let header = WireHeader::from_reader(reader, DICT_HELLO_BODY_BYTES as u64)
        .await
        .map_err(ReadWireDataError::ReadHeaderFailure)?;
    if header.message_type != message_type {
        return Err(ReadWireDataError::UnknownMessageType(header.message_type));
    }
    if header.version != PROTOCOL_VERSION_V4 || header.compressed_length != DICT_HELLO_BODY_BYTES {
        return Err(ReadWireDataError::ReadHeaderFailure(WireError::MalformedFrame(format!(
            "dictionary hello must be a {DICT_HELLO_BODY_BYTES}-byte V4 body, got {} bytes at version {}",
            header.compressed_length, header.version
        ))));
    }
    header.read_fixed_size(reader).await.map_err(ReadWireDataError::ReadBodyFailure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V5, WIRE_HEADER_SIZE};
    use futures_lite::future::block_on;

    fn hello(fill: u8) -> DictHello {
        let mut dict_sha256 = [fill; 32];
        dict_sha256[31] = fill.wrapping_add(1);
        DictHello { dict_sha256 }
    }

    fn frame(h: &DictHello, message_type: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        block_on(write_dict_hello(&mut buf, h, message_type)).unwrap();
        buf
    }

    #[test]
    fn hello_is_a_fixed_49_byte_uncompressed_frame() {
        let buf = frame(&hello(7), DICT_HELLO_REQUEST_TYPE_ID);
        assert_eq!(buf.len(), WIRE_HEADER_SIZE + DICT_HELLO_BODY_BYTES as usize);
        assert_eq!(buf[16], 0, "compression byte must be None");
        assert_eq!(&buf[WIRE_HEADER_SIZE..], &hello(7).dict_sha256);
    }

    #[test]
    fn hello_round_trips_both_directions() {
        for type_id in [DICT_HELLO_REQUEST_TYPE_ID, DICT_HELLO_RESPONSE_TYPE_ID] {
            let buf = frame(&hello(3), type_id);
            let got = block_on(read_dict_hello(&mut buf.as_slice(), type_id)).unwrap();
            assert_eq!(got, hello(3));
        }
    }

    #[test]
    fn a_frame_of_another_type_is_refused() {
        let buf = frame(&hello(3), DICT_HELLO_RESPONSE_TYPE_ID);
        let err = block_on(read_dict_hello(&mut buf.as_slice(), DICT_HELLO_REQUEST_TYPE_ID)).unwrap_err();
        assert!(matches!(err, ReadWireDataError::UnknownMessageType(DICT_HELLO_RESPONSE_TYPE_ID)));
    }

    /// An old leader opens with a ReplicationBatch. Its header announces a body far past the
    /// hello bound, so it's refused off the header alone.
    #[test]
    fn an_oversized_first_frame_is_refused_from_its_header() {
        let mut buf = frame(&hello(3), DICT_HELLO_REQUEST_TYPE_ID);
        buf[4..8].copy_from_slice(&100u32.to_le_bytes());
        buf[8..12].copy_from_slice(&(1u32 << 20).to_le_bytes());
        buf[12..16].copy_from_slice(&(1u32 << 20).to_le_bytes());
        let err = block_on(read_dict_hello(&mut buf.as_slice(), DICT_HELLO_REQUEST_TYPE_ID)).unwrap_err();
        assert!(matches!(err, ReadWireDataError::ReadHeaderFailure(WireError::MessageTooLarge { .. })));
    }

    /// A short body would otherwise decode as a sha padded with zeros.
    #[test]
    fn a_short_body_is_refused() {
        let mut buf = frame(&hello(3), DICT_HELLO_REQUEST_TYPE_ID);
        buf[8..12].copy_from_slice(&31u32.to_le_bytes());
        buf[12..16].copy_from_slice(&31u32.to_le_bytes());
        buf.truncate(buf.len() - 1);
        let err = block_on(read_dict_hello(&mut buf.as_slice(), DICT_HELLO_REQUEST_TYPE_ID)).unwrap_err();
        assert!(matches!(err, ReadWireDataError::ReadHeaderFailure(WireError::MalformedFrame(_))));
    }

    #[test]
    fn a_v5_hello_is_refused() {
        let mut buf = frame(&hello(3), DICT_HELLO_REQUEST_TYPE_ID);
        buf[0..4].copy_from_slice(&PROTOCOL_VERSION_V5.to_le_bytes());
        let err = block_on(read_dict_hello(&mut buf.as_slice(), DICT_HELLO_REQUEST_TYPE_ID)).unwrap_err();
        assert!(matches!(err, ReadWireDataError::ReadHeaderFailure(WireError::MalformedFrame(_))));
    }

    #[test]
    fn equal_shas_agree() {
        assert_eq!(check_dict_agreement(&hello(9).dict_sha256, &hello(9)), Ok(()));
    }

    #[test]
    fn different_shas_are_a_mismatch_naming_both() {
        let local = hello(0xab).dict_sha256;
        let err = check_dict_agreement(&local, &hello(0xcd)).unwrap_err();
        assert_eq!(err, DictMismatch { local_sha256: local, peer_sha256: hello(0xcd).dict_sha256 });
        for text in [err.to_string(), format!("{err:?}")] {
            assert!(text.contains(&Sha256Hex(local).to_string()), "{text}");
            assert!(text.contains(&Sha256Hex(hello(0xcd).dict_sha256).to_string()), "{text}");
        }
    }

    /// Only the sha is compared: shas differing in a single bit disagree.
    #[test]
    fn a_single_bit_difference_is_a_mismatch() {
        let local = hello(1).dict_sha256;
        let mut peer = hello(1);
        peer.dict_sha256[0] ^= 1;
        assert!(check_dict_agreement(&local, &peer).is_err());
    }
}
