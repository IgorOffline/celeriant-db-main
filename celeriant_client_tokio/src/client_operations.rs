use std::collections::HashMap;

use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, ReadRequest, RegisterSchemaRequest, SingleAggregateWrite,
    TrimStartRequest, WriteRequest,
};
use celeriant_msg::response::responses::{
    AggregateDetailsResponse, DeleteResponse, ReadResponse, RegisterSchemaResponse, TrimStartResponse, WriteResponse,
};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

use crate::celeriant_client::CeleriantClient;
use crate::client_error::ClientError;

/// Options for `write_events_with`. All fields default to the same values used by `write_events`.
///
/// The `client_id` is deliberately not an option: it scopes client-seq idempotency, so the
/// caller must always supply it explicitly. The client library never invents one.
#[derive(Debug, Clone)]
pub struct WriteEventsOptions {
    pub allow_create: bool,
    pub expected_version: Option<u64>,
    pub enforce_client_idempotency: bool,
}

impl Default for WriteEventsOptions {
    fn default() -> Self {
        Self {
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        }
    }
}

impl CeleriantClient {
    pub async fn read(&mut self, request: ReadRequest) -> Result<ReadResponse, ClientError> {
        match self.send_owned(ClientRequest::Read(request)).await? {
            ClientResponse::Read(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn write(&mut self, request: WriteRequest) -> Result<WriteResponse, ClientError> {
        match self.send_owned(ClientRequest::Write(request)).await? {
            ClientResponse::Write(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn delete(&mut self, request: DeleteRequest) -> Result<DeleteResponse, ClientError> {
        match self.send_owned(ClientRequest::Delete(request)).await? {
            ClientResponse::Delete(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn trim_start(&mut self, request: TrimStartRequest) -> Result<TrimStartResponse, ClientError> {
        match self.send_owned(ClientRequest::TrimStart(request)).await? {
            ClientResponse::TrimStart(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    pub async fn aggregate_details(
        &mut self,
        request: AggregateDetailsRequest,
    ) -> Result<AggregateDetailsResponse, ClientError> {
        match self.send_owned(ClientRequest::AggregateDetails(request)).await? {
            ClientResponse::AggregateDetails(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }

    /// Convenience method: write events to a single aggregate without constructing a `WriteRequest`.
    ///
    /// `client_id` scopes client-seq idempotency — use a stable id per logical writer, never a
    /// fresh random value per call. Idempotency enforcement is opt-in: use `write_events_with`
    /// with `enforce_client_idempotency: true` to enable it.
    pub async fn write_events(
        &mut self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
        client_id: u128,
    ) -> Result<WriteResponse, ClientError> {
        self.write_events_with(aggregate_key, events, client_id, WriteEventsOptions::default()).await
    }

    /// Like `write_events` but accepts options to control idempotency, optimistic concurrency, etc.
    pub async fn write_events_with(
        &mut self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
        client_id: u128,
        options: WriteEventsOptions,
    ) -> Result<WriteResponse, ClientError> {
        let mut writes = HashMap::new();
        writes.insert(aggregate_key, SingleAggregateWrite {
            events,
            allow_create: options.allow_create,
            expected_version: options.expected_version,
            enforce_client_idempotency: options.enforce_client_idempotency,
        });
        self.write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        })
        .await
    }

    pub async fn register_schema(
        &mut self,
        request: RegisterSchemaRequest,
    ) -> Result<RegisterSchemaResponse, ClientError> {
        match self.send_owned(ClientRequest::RegisterSchema(request)).await? {
            ClientResponse::RegisterSchema(r) => Ok(r),
            _ => Err(ClientError::ProtocolError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_events_idempotency_is_off_by_default() {
        assert!(!WriteEventsOptions::default().enforce_client_idempotency);
    }

    #[test]
    fn write_events_doc_notes_opt_in_idempotency() {
        let source = include_str!("client_operations.rs");
        let needle = ["Idempotency enforcement is ", "opt-in"].concat();
        assert!(
            source.contains(&needle),
            "write_events doc must state that idempotency enforcement is opt-in"
        );
    }
}

/// Atomic same-shard appends and version guards. Duplicate keys reject the batch.
pub struct WriteBatch<'a> {
    target: BatchTarget<'a>,
    request: WriteRequest,
    duplicate_key: bool,
}

enum BatchTarget<'a> {
    Client(&'a mut CeleriantClient),
    Pool(&'a crate::CeleriantPool),
}

impl<'a> WriteBatch<'a> {
    fn new(target: BatchTarget<'a>, client_id: u128) -> Self {
        Self { target, request: WriteRequest { correlation_id: None, client_id, user_id: None, writes: HashMap::new() }, duplicate_key: false }
    }

    pub fn append(self, key: AggregateKey, events: Vec<DatablockAggregateEvent>) -> Self {
        self.append_with(key, events, WriteEventsOptions::default())
    }

    pub fn append_with(self, key: AggregateKey, events: Vec<DatablockAggregateEvent>, options: WriteEventsOptions) -> Self {
        self.entry(key, SingleAggregateWrite {
            events,
            allow_create: options.allow_create,
            expected_version: options.expected_version,
            enforce_client_idempotency: options.enforce_client_idempotency,
        })
    }

    /// Observe this head in the same decision as all appends, without adding an event.
    pub fn guard(self, key: AggregateKey, expected_version: u64) -> Self {
        self.entry(key, SingleAggregateWrite {
            events: Vec::new(),
            allow_create: false,
            expected_version: Some(expected_version),
            enforce_client_idempotency: false,
        })
    }

    fn entry(mut self, key: AggregateKey, write: SingleAggregateWrite) -> Self {
        match self.request.writes.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => { entry.insert(write); }
            std::collections::hash_map::Entry::Occupied(_) => self.duplicate_key = true,
        }
        self
    }

    pub async fn send(self) -> Result<WriteResponse, ClientError> {
        if self.duplicate_key {
            return Err(ClientError::ProtocolError);
        }
        match self.target {
            BatchTarget::Client(client) => client.write(self.request).await,
            BatchTarget::Pool(pool) => pool.write(self.request).await,
        }
    }
}

impl CeleriantClient {
    pub fn write_batch(&mut self, client_id: u128) -> WriteBatch<'_> {
        WriteBatch::new(BatchTarget::Client(self), client_id)
    }
}

impl crate::CeleriantPool {
    pub fn write_batch(&self, client_id: u128) -> WriteBatch<'_> {
        WriteBatch::new(BatchTarget::Pool(self), client_id)
    }
}
