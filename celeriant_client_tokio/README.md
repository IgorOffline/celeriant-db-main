# Celeriant Rust Client

Async Tokio client for [Celeriant](https://celeriant.io), the distributed event store for event sourcing.

Celeriant lets you enforce business invariants at write time across multiple streams, without distributed transactions. Optimistic concurrency control, strict ordering, exactly-once writes, schema validation, cluster wide durability. All built in.

- [Website](https://celeriant.io)
- [Documentation](https://docs.celeriant.io)
- [GitHub](https://github.com/celeriant/celeriant-db)

Single connection client, connection pool with leader routing and failover, streaming iterators, and real-time watch connections.

## Install

```bash
cargo add celeriant_client_tokio
```

## Quick start

### 1. Start the server

Celeriant uses io_uring, so the container needs `seccomp=unconfined`.

```bash
docker run -d --name celeriant \
  --security-opt seccomp=unconfined \
  -p 10000:10000 \
  ghcr.io/celeriant/celeriant \
  --standalone --data-root /var/lib/celeriant --num-shards 1
```

### 2. Connect and write an event

```rust
use celeriant_client_tokio::{CeleriantClient, json_event};
use celeriant_wal::aggregate_key::AggregateKey;

let mut client = CeleriantClient::connect("localhost:10000").await?;

let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
let events = vec![json_event(1, &OrderPlaced { order_id, amount: 99.95 })?];

let my_client_id: u128 = 1; // stable id per writing service, used for idempotency tracking
client.write_events(key, events, my_client_id).await?;
```

### 3. Read it back

```rust
use celeriant_client_tokio::from_json;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;

let response = client.read(ReadRequest {
    correlation_id: None,
    aggregate_key: key,
    filters: ReadFilters::new(1),
}).await?;

let order: OrderPlaced = from_json(&response.event_batches[0].events[0])?;
```

## Atomic appends and guards

Version 0.3.0 uses client protocol 4 (bincode). Every connection completes a version-checked Identify before its first operation, even without authentication. Explicit `identify()` supplies credentials without an additional unsigned handshake. Reconnects repeat the check; legacy servers and later version changes fail without automatic downgrade or mutation replay. The server also supports protocol 5 (MessagePack).

Use the same builder on a client or pool; all keys must route to one shard:

```rust
let response = client.write_batch(my_client_id)
    .append(order_key, events)
    .guard(fence_key, observed_fence_version)
    .send().await?;
```

`.append_with(key, events, WriteEventsOptions)` controls append flags and expected versions. `.guard(key, version)` supplies empty events and disables creation and client idempotency. Duplicate keys reject the builder with `ClientError::ProtocolError` before sending; a later entry never overwrites a guard. Raw `write(WriteRequest)` retains `Result<WriteResponse, ClientError>`.

Guards do not append events or change aggregate versions. A missing aggregate passes only a zero guard; deleted aggregates reject guards. A guard-only batch returns no aggregate version; one append plus guards returns the append's committed version. A successful guard is a conditional observation, not a reservation: later writes can change it, retries after a lost response can fail OCC, and delete/recreate can reuse versions. Durability follows the server's standalone or replicated contract.

Write OCC errors use `ClientError::Server(ServerError::Write { kind: WriteError::OptimisticConcurrencyViolation { conflicts, expected_version, current_aggregate_version }, .. })`; delete uses the equivalent `DeleteError` variant. `conflicts` contains every OCC mismatch from the same validation decision, sorted by the unsigned numeric aggregate key tuple. The optional scalar fields expose the first conflict. Conflict keys remain binary on the wire. Other validation errors can take precedence over OCC. A valid OCC response leaves its connection reusable and is never automatically replayed; malformed or mismatched replies retire it.

Before submission, clients reserve `25 + 74 * N` response body bytes, where `N` counts every entry with an expected version, including guards. A request exceeding the client's receive ceiling fails locally even if its pins would succeed. Servers independently enforce their configured response ceiling. The client defaults to 64 MiB responses; setting response size equal to request size does not guarantee admission. Canonical server errors include malformed guard 2014, write response capacity 2015, and delete response capacity 4008.

## Connection pool

`CeleriantPool` manages connections and routes both writes and reads to the leader, so reads see your own writes. Follower reads are an explicit opt-in via `route_reads_to_followers`; if every follower is down they fall back to the leader instead of failing. Implements `CeleriantPoolApi` for testing and DI.

```rust
use celeriant_client_tokio::{CeleriantPool, PoolOptions};

let pool = CeleriantPool::new(PoolOptions::new("localhost:10000")
    .with_seed_addresses(vec!["localhost:10002".into()]));

pool.write(request).await?;
let response = pool.read(read_request).await?;
```

The pool also supports `delete`, `trim_start`, `aggregate_details`, `register_schema`, `watch`, `read_all` (streaming), and listing operations (`list_orgs`, `list_aggregate_types`, `list_aggregates`).

## TLS / mTLS

```rust
use celeriant_client_tokio::ClientTlsConfig;
use std::path::Path;

let tls = ClientTlsConfig::from_paths(
    Path::new("ca.crt"),
    Some((Path::new("client.crt"), Path::new("client.key"))),
    "localhost",
)?;

let mut client = CeleriantClient::connect_tls("localhost:10010", tls).await?;
```

Pass `None` in place of the certificate pair for server-only TLS.

## Examples

- [celeriant_demo](https://github.com/celeriant/celeriant-db/tree/main/celeriant_demo) - browser-based banking demo. Basic read/write patterns, OCC conflicts, watch API with SSE.
- [celeriant_reference](https://github.com/celeriant/celeriant-db/tree/main/celeriant_reference) - production-grade reference API. Postgres read projections, exactly-once writes, OCC retry loops, multi-aggregate transfers.

## Running tests

```bash
# unit tests
cargo test -p celeriant_client_tokio

# integration tests (requires docker compose up -d)
cargo test -p celeriant_integration_tests
```

## License

Apache 2.0
