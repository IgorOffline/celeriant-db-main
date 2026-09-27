# Client protocol 0.3.0

These assignments are frozen for the coordinated breaking server, Rust, .NET and Go release. Internode traffic uses version 4. Persisted WAL formats retain their existing versions.

| Client version | Body codec |
| --- | --- |
| 4 | Bincode, existing fixed-width little-endian configuration |
| 5 | MessagePack, existing array representation |

Every other version is rejected before execution. The opening Identify exchange validates the version; the authentication-disabled direct-request path validates before executing its first operation. Connections pin their opening version and reject changes. Clients complete a version-checked handshake before sending mutations, including after reconnect or pool expansion. There is no downgrade.

| Response type ID | Body fields, in order |
| --- | --- |
| 13: WriteConflictResponse | correlation_id: optional u128, conflicts: vector of AggregateConflict |
| 14: DeleteConflictResponse | correlation_id: optional u128, conflicts: vector of AggregateConflict |

AggregateConflict fields are `key: AggregateKey`, `expected: u64`, `current: u64`. The key retains the codec's existing binary representation. Lists are nonempty, unique by key, and sorted by unsigned numeric `(org_id, aggregate_type_id, aggregate_id)`. No JSON key, GenericError envelope or legacy fallback is permitted. Numeric client classifications remain write 2003 and delete 4002. Write 2014 is reserved for MalformedGuardEntry; write 2015 and delete 4008 are reserved for conflict response capacity admission failures.

The 17-byte frame header is unchanged. Both conflict responses use the variable response writer, including its small-body uncompressed path. Both compressed and uncompressed lengths must fit the configured ceiling. Compression expansion falls back to the fitting uncompressed body.

Before mutation, reserve `25 + 74 * N` uncompressed body bytes with checked arithmetic, where N counts all entries with expected versions. This admission limit applies even when all pins would succeed. Servers check their response ceiling; clients independently check their receive ceiling. Defaults are 16 MiB server request and 64 MiB server response. Matching request and response limits alone does not prove a response fits.

The exact bincode bound is `25 + 64*N`; MessagePack is at most `24 + 74*N`. Shared fixture assignments live in `fixtures/client-protocol/assignments.txt`; final codec tests must verify maximal unsigned values and vector boundaries against these bounds.
