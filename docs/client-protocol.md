# Client protocol 0.3.0

These assignments are frozen for the coordinated breaking server, Rust, .NET and Go release. Internode traffic uses version 4. Persisted WAL formats retain their existing versions.

| Client version | Body codec |
| --- | --- |
| 4 | Bincode, existing fixed-width little-endian configuration |
| 5 | MessagePack, existing array representation |

Every other version is rejected before execution. Every client connection opens with an Identify, whatever the server requires, and the exchange validates the version. Any other first frame gets a `ProtocolError` response (type 6) and the connection closes. Connections pin their opening version and reject changes. Clients complete a version-checked handshake before sending mutations, including after reconnect or pool expansion. There is no downgrade.

The Identify request carries `known_dict_sha256`. The response carries the server's `compression_dict_sha256`, plus `compression_dict_bytes` only when the shas differ. Clients seed with the built-in dictionary's sha, so a built-in server never ships bytes. The dictionary ceiling is 1 MiB (1,048,576 bytes), and the handshake has its own read limit, independent of the caller's response cap:

| Handshake read | Limit |
| --- | --- |
| V4 IdentifyResponse, client read | ceiling + 128 |
| V5 IdentifyResponse, server write | 2 × ceiling + 128 (MessagePack encodes the dictionary as an integer array; a byte ≥ 0x80 takes 2 bytes) |
| Non-Identify reply to an Identify | min(caller cap, the Identify limit), checked from the header before any body read |

Data frames stay under the caller's cap.

| Response type ID | Body fields, in order |
| --- | --- |
| 13: WriteConflictResponse | correlation_id: optional u128, conflicts: vector of AggregateConflict |
| 14: DeleteConflictResponse | correlation_id: optional u128, conflicts: vector of AggregateConflict |

AggregateConflict fields are `key: AggregateKey`, `expected: u64`, `current: u64`. The key retains the codec's existing binary representation. Lists are nonempty, unique by key, and sorted by unsigned numeric `(org_id, aggregate_type_id, aggregate_id)`. No JSON key, GenericError envelope or legacy fallback is permitted. Numeric client classifications remain write 2003 and delete 4002. Write 2014 is reserved for MalformedGuardEntry; write 2015 and delete 4008 are reserved for conflict response capacity admission failures.

The 17-byte frame header is unchanged. Both conflict responses use the variable response writer, including its small-body uncompressed path. Both compressed and uncompressed lengths must fit the configured ceiling. Compression expansion falls back to the fitting uncompressed body.

Before mutation, reserve `25 + 74 * N` uncompressed body bytes with checked arithmetic, where N counts all entries with expected versions. This admission limit applies even when all pins would succeed. Servers check their response ceiling; clients independently check their receive ceiling. Defaults are 16 MiB server request and 64 MiB server response. Matching request and response limits alone does not prove a response fits.

The exact bincode bound is `25 + 64*N`; MessagePack is at most `24 + 74*N`. Shared fixture assignments live in `fixtures/client-protocol/assignments.txt`; final codec tests must verify maximal unsigned values and vector boundaries against these bounds.
