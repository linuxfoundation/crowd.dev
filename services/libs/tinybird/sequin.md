# SEQUIN ARCHITECTURE DOCUMENTATION

## 1. OVERVIEW & PURPOSE

```
                              ┌── topic: activityRelations ───► Kafka Connect Connectors (Tinybird, Snowflake)
                              │
PostgreSQL ──► Sequin ────────┼── topic: members ─────────────► Kafka Connect Connectors (Tinybird, Snowflake)
  (source)    (CDC engine)    │
                              ├── topic: organizations ───────► Kafka Connect Connectors (Tinybird, Snowflake)
                              │
                              ├── topic: integrations ────────► Kafka Connect Connectors (Tinybird, Snowflake)
                              │
                              └── ...
```

Sequin is a **Change Data Capture (CDC) system** that streams changes from PostgreSQL databases to various sinks (Kafka, HTTP endpoints, SQS, Redis, etc.). It uses PostgreSQL's logical replication feature to capture database changes in real-time and deliver them to configured consumers with at-least-once delivery guarantees. We're currently only using the kafka sinks.

### Key Capabilities
- Real-time streaming of PostgreSQL changes via logical replication
- Multiple sink types (HTTP, Kafka, SQS, Redis, RabbitMQ, NATS, etc.)
- Backfill support for historical data
- Message transformation and filtering
- At-least-once delivery guarantees with idempotency
- Horizontal scalability via partitioning

---

## 2. POSTGRESQL LOGICAL REPLICATION CONCEPTS

### 2.1 Replication Slots

**Reference:** [PostgreSQL Replication Slots](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS)

**What is a Replication Slot?**
A PostgreSQL replication slot is a server-side mechanism that:
- Tracks which WAL (Write-Ahead Log) data has been consumed
- Prevents PostgreSQL from removing WAL segments before they're consumed
- Provides a stable position in the WAL stream to resume from

**File Location:** `sequin/lib/sequin/replication/postgres_replication_slot.ex`

**Key Fields:**
- `slot_name`: Unique identifier for the slot
- `publication_name`: PostgreSQL publication defining which tables to replicate
- `status`: `:active` or `:disabled`
- `partition_count`: Number of partitions for message distribution

### 2.2 Write-Ahead Log (WAL)

**Reference:** [PostgreSQL Write-Ahead Logging](https://www.postgresql.org/docs/current/wal-intro.html)

The WAL is PostgreSQL's transaction log where all changes are written before being applied to the database. Sequin reads from the WAL via logical replication.

WAL data is stored as a sequence of **segment files** on disk (typically 16 MB each) in the `pg_wal/` directory of the PostgreSQL data folder. Each segment file is named by its position in the stream (e.g., `000000010000000000000001`). Internally, WAL records are binary — they contain the raw before/after images of changed data pages. PostgreSQL uses a **logical decoding** layer (see [2.5 pgoutput Protocol](#25-pgoutput-protocol)) to translate these raw binary records into structured, row-level change events that Sequin can consume.

### 2.3 LSN (Log Sequence Number)

An LSN is a 64-bit integer representing a position in the PostgreSQL WAL. Format: `1AB/2CD34567` (hex). The part before the slash (`1AB`) is the **WAL segment file number** (high 32 bits), identifying which physical WAL file contains this position. The part after the slash (`2CD34567`) is the **byte offset** (low 32 bits) within that segment. Both are hexadecimal. The full 64-bit value is `(segment << 32) | offset`.

**Reference:** [PostgreSQL WAL Internals — pg_lsn Type](https://www.postgresql.org/docs/current/datatype-pg-lsn.html)

**Key LSN concepts:**
- `commit_lsn`: LSN when a transaction committed
- `restart_lsn`: Position from which to restart replication
- `confirmed_flush_lsn`: LSN confirmed as processed by Sequin

**File:** `sequin/lib/sequin/postgres.ex` (helper functions for LSN manipulation)

### WAL LSN Timeline

```
WAL position (LSN) ─────────────────────────────────────────────────────────►

  restart_lsn              confirmed_flush_lsn                     current WAL write position
      │                          │                                          │
      ▼                          ▼                                          ▼
──────●──────────────────────────●──────────────────────────────────────────●───
      │                          │                                          │
      │◄── checkpoint lag ──────►│◄───────── unprocessed WAL lag ──────────►│
      │                          │                                          │
      │                          │                                          │
      │  Postgres streams        │  Sequin's cursor.                        │  Where new writes
      │  from HERE on            │  Everything between                      │  are landing now.
      │  reconnect.              │  restart_lsn and here                    │
      │                          │  is "already processed"                  │
      │                          │  but Postgres still sends it.            │
      │                                                                     │
      │◄────────────────────── total ──────────────────────────────────────►│
      │                    (reported as "replication lag")                  │
```

### Long-Running Transaction Pinning (and why they are really bad)

```
Checkpoint C1     Txn T starts     Checkpoint C2     Checkpoint C3     Txn T commits
    │                 │                 │                 │                 │
    ▼                 ▼                 ▼                 ▼                 ▼
────●─────────────────●─────────────────●─────────────────●─────────────────●────
    │                                                                       │
    │  restart_lsn pinned here                                              │
    │  (last checkpoint before T's BEGIN)                                   │
    │                                                                       │
    │  C2 and C3 happen fine, but the        Only after T commits AND       │
    │  slot can't use them — restarting       client confirms it,           │
    │  from C2 would miss T's BEGIN           restart_lsn can advance       │
```

This means that, long running transactions prevent the replication slot from advancing, and replication slot increases indefinitely.

### 2.4 Replica Identity

**Reference:** [PostgreSQL Replica Identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY)

Replica identity controls **what data PostgreSQL includes in UPDATE and DELETE WAL messages**. By default, only the primary key is sent for deletes, and only changed columns + primary key for updates.

**Modes:**
- `DEFAULT` — Primary key columns only (for identifying the row)
- `FULL` — All columns, both old and new values
- `NOTHING` — No old row identity (deletes/updates can't be identified)
- `INDEX` — Use a specific unique index instead of primary key

**Why this matters for Sequin:**
Sequin typically needs `FULL` replica identity to populate `old_fields` on updates and to provide complete row data on deletes. Without it, the `changes` field in ConsumerEvent data and TOAST column resolution may have missing values.

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

### 2.5 pgoutput Protocol

**Reference:** [PostgreSQL Logical Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)

Sequin uses PostgreSQL's built-in `pgoutput` logical decoding output plugin. This plugin converts raw WAL bytes into a structured binary protocol with typed messages.

Each message starts with a single byte identifying its type. In the codebase, these are matched using Elixir's character literal syntax (`?X` = the ASCII byte value of character `X`):

| Protocol byte | Elixir | Meaning |
|---|---|---|
| `B` (66) | `?B` | Begin transaction |
| `C` (67) | `?C` | Commit transaction |
| `I` (73) | `?I` | Insert |
| `U` (85) | `?U` | Update |
| `D` (68) | `?D` | Delete |
| `R` (82) | `?R` | Relation (table schema) |
| `M` (77) | `?M` | Logical message |

The Processor (section 4.2) decodes these binary messages into typed Elixir structs.

### 2.6 Publications

**Reference:** [PostgreSQL Logical Replication — Publications](https://www.postgresql.org/docs/current/logical-replication-publication.html)

A publication defines **which tables** a replication slot streams changes for. It acts as a filter on the WAL — only changes to published tables are sent to the subscriber.

**Creating a publication:**
```sql
-- Specific tables
CREATE PUBLICATION my_pub FOR TABLE users, orders;

-- All tables
CREATE PUBLICATION my_pub FOR ALL TABLES;
```

A replication slot is paired with a publication. When Sequin creates a replication slot, it also creates (or references) a publication. The slot then only streams changes for tables included in that publication.

### 2.7 WAL Cursors (Sequin-specific)

WAL cursors are a **Sequin-specific concept**, not a PostgreSQL primitive. PostgreSQL only provides LSNs (a single position in the WAL), but one transaction (one LSN) can contain many row changes. Sequin adds a `commit_idx` counter to distinguish individual messages within the same transaction.

```elixir
%{
  commit_lsn: 123456789,  # Transaction's LSN (from PostgreSQL)
  commit_idx: 5           # Message index within transaction (0-based, Sequin-assigned)
}
```

**Why both fields?**
A single transaction can contain multiple changes. The `commit_idx` distinguishes between them. PostgreSQL has no equivalent — it only tracks the transaction-level LSN.

---

## 3. CORE ARCHITECTURE COMPONENTS

### 3.1 Supervision Tree

```
Sequin.Application
├── Sequin.Repo (Database)
├── ... (Vault, PubSub, Finch, Cache, Oban, ConnectionCaches, Web endpoints, etc.)
├── Sequin.Runtime.MutexedSupervisor
│   └── Sequin.Runtime.Supervisor
│       ├── Sequin.Runtime.Starter (GenServer - manages startup/lifecycle)
│       ├── Sequin.DynamicSupervisor (TableReaderServerSupervisor)
│       │   └── TableReaderServer (GenStateMachine, per active backfill)
│       ├── Sequin.DynamicSupervisor (SlotSupervisorSupervisor)
│       │   └── SlotSupervisor (DynamicSupervisor, per replication slot)
│       │       ├── SlotProducer.Supervisor (strategy: :one_for_all)
│       │       │   ├── SlotProducer (GenStage producer)
│       │       │   ├── Processor (N partitions, GenStage producer_consumer)
│       │       │   ├── ReorderBuffer (GenStage consumer)
│       │       │   └── SlotProcessorServer (GenServer)
│       │       ├── SlotMessageStoreSupervisor (per consumer, strategy: :one_for_all)
│       │       │   └── SlotMessageStore (per partition, GenServer)
│       │       └── SinkPipeline (Broadway pipeline per consumer, skipped for sequin_stream)
│       │           ├── ConsumerProducer (Broadway producer)
│       │           ├── Processors (Broadway processors)
│       │           └── Batchers (Broadway batchers)
│       └── Sequin.DynamicSupervisor (WalEventSupervisor)
│           └── WalPipelineServer (GenStateMachine)
```

**Key Files:**
- `sequin/lib/sequin/application.ex`
- `sequin/lib/sequin/runtime/supervisor.ex`
- `sequin/lib/sequin/runtime/slot_supervisor.ex`

### 3.2 Message Flow Architecture

```
PostgreSQL WAL
    ↓
[1] SlotProducer (connects via Postgrex.Protocol)
    ↓ (binary WAL messages)
[2] Processor (N partitions - decode & transform)
    ↓ (SlotProcessor.Message structs)
[3] ReorderBuffer (reorder from partitions)
    ↓ (Batches)
[4] SlotProcessorServer (batch coordinator)
    ↓ (distribute to consumers)
[5] MessageHandler (filter & transform)
    ↓ (ConsumerEvent/ConsumerRecord)
[6] SlotMessageStore (per-consumer buffer)
    ↓ (on-demand)
[7] ConsumerProducer (Broadway producer)
    ↓ (demand-driven)
[8] SinkPipeline (Broadway processors & batchers)
    ↓ (batched messages)
[9] Sink-specific handler (HTTP, Kafka, SQS, etc.)
    ↓
External Sink
```

---

## 4. DETAILED COMPONENT DESCRIPTIONS

**Repository:** [https://github.com/sequinstream/sequin](https://github.com/sequinstream/sequin)

### 4.1 SlotProducer

**File:** `sequin/lib/sequin/runtime/slot_producer/slot_producer.ex`

**Purpose:** GenStage producer that establishes a replication connection to PostgreSQL and streams WAL messages.

**Key Responsibilities:**
1. Connects to PostgreSQL using `Postgrex.Protocol` in replication mode
2. Sends `START_REPLICATION` command
3. Receives binary WAL messages from PostgreSQL
4. Manages demand from downstream consumers (backpressure)
5. Periodically sends ACK messages back to PostgreSQL
6. Tracks `restart_wal_cursor` for crash recovery
7. Emits batch markers for downstream batching

**Important State:**
```elixir
%State{
  protocol: Postgrex.Protocol.state(),  # Replication connection
  restart_wal_cursor: %{commit_lsn: ..., commit_idx: ...},  # Resume point
  last_dispatched_wal_cursor: ...,  # High watermark
  demand: integer(),  # Downstream demand
  accumulated_messages: %{messages: [...], count: ..., bytes: ...},
  status: :active | :buffering | :disconnected,
  batch_idx: integer()  # Current batch number
}
```

**Buffering States:**
- `:active` - Producing messages as demand arrives
- `:buffering` - No demand, buffering incoming socket messages
- `:disconnected` - Connection lost, will retry

**Message Types Handled:**
- `?B` (Begin) - Sets up transaction context
- `?C` (Commit) - Closes transaction  
- `?I`, `?U`, `?D` (Insert/Update/Delete) - Data changes
- `?R` (Relation) - Schema changes, forwarded to consumers
- `?M` (Logical) - Custom messages (heartbeats, transaction annotations)
- `?k` (Keepalive) - Connection health check

**Restart/Recovery:**
When SlotProducer crashes and restarts:
1. Fetches `restart_wal_cursor` from database
2. Queries PostgreSQL's `pg_replication_slots` for `restart_lsn`
3. Starts replication from that LSN
4. Skips messages before `restart_wal_cursor` (using guard `below_restart_wal_cursor?/1`)

### 4.2 Processor

**File:** `sequin/lib/sequin/runtime/slot_producer/processor.ex`

**Purpose:** GenStage producer_consumer that decodes binary WAL messages and transforms them into `SlotProcessor.Message` structs. Runs in N parallel partitions (one per CPU core).

**Key Responsibilities:**
1. Decode binary payloads using `PostgresAdapter.Decoder`
2. Cast PostgreSQL types to Elixir types
3. Extract primary keys from row data
4. Build `SlotProcessor.Message` structs with typed fields
5. Filter out internal Sequin tables
6. Forward batch markers to ReorderBuffer

**Partition Count:** `System.schedulers_online()` (typically 8-16)

**Message Transformation:**
```elixir
# Input: SlotProducer.Message (binary payload)
%Message{
  payload: <<binary_wal_data>>,
  commit_lsn: 123456,
  commit_idx: 5,
  batch_idx: 10
}

# Output: SlotProcessor.Message (typed)
%SlotProcessor.Message{
  action: :insert | :update | :delete,
  table_oid: 16384,
  table_schema: "public",
  table_name: "users",
  ids: ["user-123"],  # Primary keys
  fields: [%Field{column_name: "name", value: "Alice"}],
  old_fields: [...],  # For updates/deletes
  commit_lsn: 123456,
  commit_idx: 5,
  trace_id: "uuid",
  idempotency_key: "base64(lsn:idx)"
}
```

**Type Casting:**
Uses `Sequin.Postgres.ValueCaster` to convert PostgreSQL wire format to Elixir types (strings, integers, JSON, etc.).

### 4.3 ReorderBuffer

**File:** `sequin/lib/sequin/runtime/slot_producer/reorder_buffer.ex`

**Purpose:** GenStage consumer that receives messages from multiple Processor partitions and reorders them by `batch_idx` before flushing complete batches.

**Why Reordering?**
Messages are processed in parallel across N partitions. They may arrive out-of-order. ReorderBuffer ensures batches are delivered sequentially.

**Key Responsibilities:**
1. Collect messages from all N Processor partitions
2. Group messages by `batch_idx`
3. Wait for batch markers from ALL partitions before marking batch as ready
4. Flush batches in sequential order to SlotProcessorServer
5. Implement backpressure based on ready batch count
6. System health checks (memory, disk)

**State:**
```elixir
%State{
  pending_batches_by_idx: %{
    100 => %Batch{messages: [...], markers_received: [0, 1]}  # Waiting for marker from partition 2
  },
  ready_batches_by_idx: %{
    99 => %Batch{messages: [...], markers_received: [0, 1, 2]}  # Ready to flush
  },
  producer_subscriptions: [%{producer: pid, demand: 500}],
  producer_partition_count: 8,
  setting_max_ready_batches: 2  # Backpressure threshold
}
```

**Batch Marker:**
```elixir
%BatchMarker{
  idx: 100,  # Batch number
  high_watermark_wal_cursor: %{commit_lsn: ..., commit_idx: ...},
  producer_partition_idx: 0  # Which partition sent this marker
}
```

**Backpressure:**
- If `ready_batches_by_idx` count exceeds `setting_max_ready_batches`, stops asking for demand
- Resumes demand after successful batch flush

### 4.4 SlotProcessorServer

**File:** `sequin/lib/sequin/runtime/slot_processor_server.ex`

**Purpose:** Central coordinator GenServer that receives batches from ReorderBuffer and distributes messages to consumers.

**Key Responsibilities:**
1. Receive batches from ReorderBuffer (via `handle_batch/2` call)
2. Call `MessageHandler.handle_messages/2` to filter and route messages
3. Monitor SlotMessageStore processes (crash detection)
4. Emit heartbeat messages to PostgreSQL
5. Verify heartbeat reception (detect stale connections)
6. Track high watermark for restart cursor calculation

**Heartbeat Mechanism:**
Every 15 seconds (configurable):
1. Emit a logical message to PostgreSQL with heartbeat ID and timestamp
2. Message flows through replication → SlotProducer → ... → back to SlotProcessorServer
3. On receipt, cancel current heartbeat and schedule next one
4. Every 30 seconds, verify heartbeat health

**Heartbeat Verification States:**
- `:ok` - Heartbeat received within 5 min
- `:no_recent_heartbeat` - No heartbeat emitted recently (bug)
- `:stale_connection` - Heartbeat sent but not received in 10 min
- `:lsn_advanced` - LSN passed heartbeat without receiving it
- `:no_last_commit_lsn` - No messages received yet

**Monitoring SlotMessageStores:**
Monitors the first partition of each consumer's SlotMessageStore. If it crashes, SlotProcessorServer crashes too (causing full restart via supervision tree).

**Restart Cursor Calculation:**
```elixir
def restart_wal_cursor!(state) do
  # Get minimum unpersisted cursor from all SlotMessageStores
  lows = Task.async_stream(consumers, fn consumer ->
    SlotMessageStore.min_unpersisted_wal_cursors(consumer)
  end)
  
  # Return the minimum across all stores
  Enum.min_by(lows, &{&1.commit_lsn, &1.commit_idx})
end
```

This ensures replication only advances past messages that are safely buffered in ALL consumers.

### 4.5 MessageHandler

**File:** `sequin/lib/sequin/runtime/message_handler.ex`

**Purpose:** Stateless module that transforms `SlotProcessor.Message` to `ConsumerEvent` or `ConsumerRecord` and distributes to appropriate consumers.

**Key Responsibilities:**
1. Match messages to consumers (via `Consumers.matches_message?/2`)
2. Transform to consumer-specific format (Event vs Record)
3. Handle TOAST columns (large values stored out-of-line)
4. Backfill coordination with TableReaderServer
5. Insert WalEvents for WAL pipelines
6. Put messages into SlotMessageStores

**Context:**
```elixir
%Context{
  consumers: [%SinkConsumer{}],  # Active consumers for this slot
  wal_pipelines: [%WalPipeline{}],  # WAL event destinations
  postgres_database: %PostgresDatabase{},
  replication_slot_id: "uuid"
}
```

**Message Matching:**
A message matches a consumer if:
- Consumer's source table OIDs include the message's table OID
- Message passes consumer's filter function (optional)
- Consumer is not disabled

**Consumer Message Types:**

**ConsumerEvent** (for event-based consumers):
```elixir
%ConsumerEvent{
  consumer_id: "uuid",
  commit_lsn: 123456,
  commit_idx: 5,
  table_oid: 16384,
  record_pks: ["user-123"],
  group_id: "default",  # For ordering
  state: :available | :delivered,
  ack_id: "uuid",  # For acknowledgment
  data: %ConsumerEventData{
    record: %{id: "user-123", name: "Alice"},
    changes: %{name: %{old: "Bob", new: "Alice"}},  # For updates
    action: :insert | :update | :delete,
    metadata: %{...}
  }
}
```

**ConsumerRecord** (for stream-based consumers):
```elixir
%ConsumerRecord{
  consumer_id: "uuid",
  commit_lsn: 123456,
  commit_idx: 5,
  table_oid: 16384,
  record_pks: ["user-123"],
  group_id: "default",
  state: :available | :acked | :delivered | :pending_redelivery,
  data: %ConsumerRecordData{
    record: %{id: "user-123", name: "Alice"},
    metadata: %{...}
  }
}
```

**TOAST Handling:**
PostgreSQL stores large column values (>2KB typically) in a separate TOAST table. Updates to other columns send `:unchanged_toast` markers. MessageHandler:
1. Detects `value: :unchanged_toast` in fields
2. Looks up value from `old_fields` 
3. Replaces `:unchanged_toast` with actual value

**Backfill Coordination:**
Before handling messages, MessageHandler calls `TableReaderServer.pks_seen/2` for tables with active backfills. This prevents duplicate delivery of backfilled records.

### 4.6 SlotMessageStore

**File:** `sequin/lib/sequin/runtime/slot_message_store.ex`

**Purpose:** Per-consumer, per-partition GenServer that maintains an in-memory buffer of undelivered messages with overflow to PostgreSQL.

**Key Responsibilities:**
1. Buffer incoming messages in ETS tables
2. Partition messages for parallel consumption
3. Produce messages on demand (pull-based)
4. Track message visibility (delivery timeouts)
5. Handle message acknowledgments
6. Persist overflow messages to database
7. Implement backpressure

**Partitioning:**
Each consumer can have N partitions (default 1, configurable). Messages are partitioned by:
```elixir
partition = :erlang.phash2(message.group_id, consumer.partition_count)
```

This ensures messages with the same `group_id` always go to the same partition (ordering guarantee).

**State:**
```elixir
%State{
  consumer_id: "uuid",
  partition: 0,  # Which partition this store manages
  consumer: %SinkConsumer{},
  
  # In-memory buffer (ETS)
  ets_table: :ets_table_name,
  payload_size_bytes: 5_000_000,  # Current memory usage
  max_memory_bytes: 10_000_000,   # Memory limit
  
  # Produced messages tracking
  produced: %{
    "ack-id-1" => %{message: ..., produced_at: ~U[...], producer_pid: pid}
  },
  
  # Persistence tracking
  unflushed_messages: [],  # Messages not yet in DB
  unflushed_batch_ids: MapSet.new(),  # TableReader batches not persisted
  
  # High watermark
  high_watermark_wal_cursor: %{commit_lsn: ..., commit_idx: ...}
}
```

**ETS Table Schema:**
The ETS table is an ordered_set with composite keys:
```elixir
# Key structure
{commit_lsn, commit_idx, random_suffix}

# Value
%ConsumerEvent{} or %ConsumerRecord{}
```

Random suffix ensures uniqueness for messages at the same WAL cursor (e.g., from backfills).

**Message Lifecycle:**

1. **Put Messages:**
   ```elixir
   SlotMessageStore.put_messages(consumer, messages)
   # → Insert into ETS
   # → Update payload_size_bytes
   # → If exceeds max, trigger flush to DB
   ```

2. **Produce Messages:**
   ```elixir
   SlotMessageStore.produce(consumer, count, producer_pid)
   # → Pull `count` deliverable messages from ETS
   # → Move to `produced` map with visibility timeout
   # → Return to ConsumerProducer
   ```

3. **Acknowledge Success:**
   ```elixir
   SlotMessageStore.messages_succeeded(consumer, ack_ids)
   # → Remove from `produced` map
   # → Delete from ETS
   # → Remove from DB (if persisted)
   # → Update payload_size_bytes
   ```

4. **Handle Failure:**
   ```elixir
   SlotMessageStore.messages_failed(consumer, metadatas)
   # → Update deliver_count, not_visible_until
   # → Return to ETS (if under max retries)
   # → Or persist to DB as failed
   ```

**Visibility Timeout:**
When a message is produced, it's marked with `not_visible_until = now + timeout`. This prevents re-delivery while the message is being processed. If the consumer crashes, the message becomes visible again after timeout.

**Backpressure:**
If `payload_size_bytes > max_memory_bytes`:
1. Flush oldest messages to database
2. Return `{:error, :payload_size_limit_exceeded}` to MessageHandler
3. MessageHandler retries with exponential backoff
4. SlotProcessorServer eventually backs off, pausing replication

**Persistence:**
Messages are flushed to `consumer_events` or `consumer_records` tables when:
- Memory limit exceeded
- Periodic flush timer (every 15 seconds)
- Messages older than 2 minutes
- TableReader batch completion

**High Watermark Tracking:**
```elixir
SlotMessageStore.put_high_watermark_wal_cursor(consumer, {batch_idx, wal_cursor})
# → Update state.high_watermark_wal_cursor
# → Used to calculate restart cursor
```

### 4.7 ConsumerProducer

**File:** `sequin/lib/sequin/runtime/consumer_producer.ex`

**Purpose:** Broadway producer that pulls messages from SlotMessageStore on-demand.

**Key Responsibilities:**
1. Pull messages from SlotMessageStore based on demand
2. Wrap in Broadway.Message structs
3. Configure acknowledger for success/failure handling
4. Periodic polling (every 10 seconds)
5. Trim idempotency sets

**Demand Handling:**
```elixir
def handle_demand(incoming_demand, state) do
  # Don't immediately produce - schedule it
  new_state = schedule_demand(state)
  {:noreply, [], %{state | demand: state.demand + incoming_demand}}
end

def handle_info(:handle_demand, state) do
  # Pull messages from SlotMessageStore
  messages = SlotMessageStore.produce(state.consumer, state.demand, self())
  
  # Wrap in Broadway messages
  broadway_messages = Enum.map(messages, fn msg ->
    %Broadway.Message{
      data: msg,
      acknowledger: {SinkPipeline, {consumer, test_pid, store_mod}, nil}
    }
  end)
  
  {:noreply, broadway_messages, state}
end
```

**Periodic Polling:**
Every 10 seconds, attempts to produce messages even if demand was previously zero (in case new messages arrived).

**Idempotency Trimming:**
Every 10 seconds:
1. Query PostgreSQL for `confirmed_flush_lsn` of replication slot
2. Call `MessageLedgers.trim_delivered_cursors_set/2`
3. Remove old entries from Redis idempotency set

### 4.8 SinkPipeline

**File:** `sequin/lib/sequin/runtime/sink_pipeline.ex`

**Purpose:** Broadway pipeline that processes and delivers messages to sinks (HTTP, Kafka, SQS, etc.).

**Broadway Architecture:**
```
ConsumerProducer
    ↓
[Processors] (parallel transformation)
    ↓
[Batchers] (group for delivery)
    ↓
Sink-specific handler
```

**Processing Flow:**

1. **handle_message/2:**
   - Enrich message (run enrichment functions)
   - Filter message (run filter functions)
   - Apply routing (run routing functions)
   - Call sink-specific `handle_message/2` if defined

2. **handle_batch/3:**
   - Check idempotency (skip already-delivered)
   - Call sink-specific `handle_batch/4`
   - Handle success: Mark messages as delivered in MessageLedgers
   - Handle failure: Retry logic handled by acknowledger

**Acknowledger:**
Configured to use `{SinkPipeline, {consumer, test_pid, store_mod}, nil}`:
```elixir
def ack(_ack_ref, successful, failed) do
  # Successful messages
  ack_ids = Enum.map(successful, & &1.data.ack_id)
  MessageLedgers.wal_cursors_delivered(consumer.id, wal_cursors_from(successful))
  SlotMessageStore.messages_succeeded(consumer, ack_ids)
  
  # Failed messages
  metadatas = Enum.map(failed, fn msg ->
    %{ack_id: msg.data.ack_id, ...error info...}
  end)
  SlotMessageStore.messages_failed(consumer, metadatas)
end
```

**Sink-Specific Implementations:**
Each sink type implements the SinkPipeline behavior:
- `HttpPushPipeline` - HTTP POST requests
- `KafkaPipeline` - Kafka producers
- `SqsPipeline` - AWS SQS
- `RedisPipeline` - Redis streams
- etc.

**Files:**
- `sequin/lib/sequin/runtime/http_push_pipeline.ex`
- `sequin/lib/sequin/runtime/kafka_pipeline.ex`
- `sequin/lib/sequin/runtime/sqs_pipeline.ex`
- ...

### 4.9 TableReaderServer

**File:** `sequin/lib/sequin/runtime/table_reader_server.ex`

**Purpose:** GenStateMachine that reads historical data from tables for backfills.

**Key Responsibilities:**
1. Paginate through table using keyset cursor
2. Transform rows to ConsumerEvent/ConsumerRecord
3. Emit watermark messages to WAL
4. Coordinate with MessageHandler to deduplicate
5. Track batch progress
6. Handle PRIMARY KEY deduplication

**State Machine:**
```
:initializing → :reading → :flushing → :reading → ... → :complete
```

**Backfill Process:**

1. **Initialization:**
   - Load backfill configuration
   - Determine keyset cursor columns (primary keys)
   - Set up ETS multiset for batch tracking

2. **Reading:**
   - Execute SQL with keyset pagination:
     ```sql
     SELECT * FROM table
     WHERE (pk1, pk2) > (last_pk1, last_pk2)
     ORDER BY pk1, pk2
     LIMIT page_size
     ```
   - Convert rows to ConsumerEvent/ConsumerRecord
   - Assign to batches (batch_id = UUID)
   - Put batches into SlotMessageStore

3. **Watermark Emission:**
   - After each batch, emit logical message to PostgreSQL:
     ```elixir
     pg_logical_emit_message(true, 'sequin.backfill.batch', 
       '{"batch_id": "...", "backfill_id": "...", "replication_slot_id": "..."}'
     )
     ```
   - Message flows through replication pipeline
   - SlotProcessorServer receives it, calls `TableReader.flush_batch/2`

4. **Batch Flushing:**
   - Wait for watermark message to arrive (ensures batch persisted)
   - Mark batch as complete
   - Continue to next page

5. **Deduplication:**
   - TableReaderServer maintains ETS multiset: `{table_oid → [backfill_ids]}`
   - When WAL messages arrive via MessageHandler, pks_seen/2 is called
   - PKs are removed from in-flight batches via ETS operations
   - Only unseen PKs from backfill are delivered

**Why Watermarks?**
The watermark ensures that the batch has been replicated through the WAL before marking it complete. This prevents data loss if TableReaderServer crashes.

---

## 5. DATA STRUCTURES

### 5.1 ConsumerEvent vs ConsumerRecord

**ConsumerEvent** (for push consumers - HTTP, Kafka, SQS):
- Ephemeral - deleted after successful delivery
- Simpler state machine: `:available` → `:delivered`
- Includes full change information (old/new values)

**ConsumerRecord** (for pull consumers - HTTP Pull, Sequin Stream):
- Persistent - kept for consumer to fetch
- Complex state machine: `:available` → `:delivered` → `:acked` → `:pending_redelivery`
- Current record state only (no changes)

### 5.2 WAL Cursors

```elixir
%{
  commit_lsn: 123456789,  # Integer LSN
  commit_idx: 5           # Integer index within transaction
}
```

**Comparison:**
```elixir
def compare_wal_cursors(cursor1, cursor2) do
  case {cursor1.commit_lsn, cursor1.commit_idx} do
    {lsn, idx} when {lsn, idx} < {cursor2.commit_lsn, cursor2.commit_idx} -> :lt
    {lsn, idx} when {lsn, idx} > {cursor2.commit_lsn, cursor2.commit_idx} -> :gt
    _ -> :eq
  end
end
```

### 5.3 Batch Markers

```elixir
%BatchMarker{
  idx: 100,  # Sequential batch number
  high_watermark_wal_cursor: %{commit_lsn: ..., commit_idx: ...},
  producer_partition_idx: 0  # Which Processor partition
}
```

Sent by SlotProducer to each Processor partition, which forwards to ReorderBuffer. ReorderBuffer waits for markers from ALL partitions before flushing the batch.

### 5.4 Message States

**ConsumerEvent states:**
- `:available` - Ready for delivery
- `:delivered` - Successfully delivered

**ConsumerRecord states:**
- `:available` - Ready for delivery
- `:delivered` - Sent to consumer, awaiting ack
- `:acked` - Consumer acknowledged receipt
- `:pending_redelivery` - Failed, will retry

---

## 6. STATE MANAGEMENT

### 6.1 In-Memory State

**ETS Tables:**
- `SlotMessageStore` ETS table per consumer/partition (ordered_set)
  - Stores ConsumerEvent/ConsumerRecord structs
  - Key: `{commit_lsn, commit_idx, random_suffix}`
  
- `TableReaderServer` ETS multiset per backfill
  - Stores PKs for deduplication
  - Key: `batch_id`, Values: `[pk1, pk2, ...]`
  
- Global ETS table: `table_oid_to_backfill_ids`
  - Maps table OIDs to active backfill IDs
  - Enables fast lookup in MessageHandler

**GenServer State:**
- Each process maintains its own state struct
- No shared mutable state between processes
- Communication via message passing

### 6.2 Persistent State

**PostgreSQL Tables (config schema):**
- `postgres_replication_slots` - Replication slot configuration
- `sink_consumers` - Consumer configuration
- `backfills` - Backfill jobs
- `http_endpoints` - HTTP endpoint configuration
- `postgres_databases` - Database connection info

**PostgreSQL Tables (stream schema):**
- `consumer_events` - Overflow/persisted events
- `consumer_records` - Overflow/persisted records  
- `wal_events` - WAL pipeline events

**Redis:**
- Delivered WAL cursors (sorted set): `message_ledgers:delivered_cursors:{consumer_id}`
  - Score: `commit_lsn * 1000000 + commit_idx`
  - Member: `"#{commit_lsn}:#{commit_idx}"`
  
- Undelivered WAL cursors (sorted set): `message_ledgers:undelivered_cursors:{consumer_id}`

**restart_wal_cursor Storage:**
Stored in `postgres_replication_slots` table in an `annotations` JSON column:
```elixir
%{
  "restart_wal_cursor" => %{
    "commit_lsn" => 123456789,
    "commit_idx" => 5
  }
}
```

### 6.3 High Watermark Tracking

Each SlotMessageStore maintains its own `high_watermark_wal_cursor`. This is the highest WAL cursor that has been flushed to this store.

When SlotProcessorServer calculates `restart_wal_cursor`:
1. Query ALL SlotMessageStores for their minimum unpersisted cursor
2. Take the minimum across all stores
3. This ensures the slot doesn't advance past any store's oldest message

**Why per-store watermarks?**
Different consumers may process at different speeds. A slow consumer should not block a fast consumer's watermark.

---

## 7. ERROR HANDLING & RECOVERY

### 7.1 Supervision Strategy

**SlotProducer.Supervisor:** `:one_for_all`
- If any component crashes, restart entire pipeline
- Ensures consistent state across SlotProducer, Processors, ReorderBuffer, SlotProcessorServer

**SlotSupervisor (DynamicSupervisor):** `:one_for_one`
- Children started dynamically (not statically in init)
- Each consumer's SlotMessageStore + SinkPipeline is independent
- If one consumer crashes, others continue

**SlotMessageStoreSupervisor:** `:one_for_all`
- All partitions of a consumer restart together
- Ensures consistent partition state

### 7.2 Restart Strategies

**SlotProducer Restart:**
1. Fetch `restart_wal_cursor` from database
2. Connect to PostgreSQL replication
3. Send `START_REPLICATION` from `restart_lsn`
4. Skip messages before `restart_wal_cursor` (they're already buffered)

**SlotMessageStore Restart:**
1. Load persisted messages from database
2. Populate ETS table
3. Resume producing to ConsumerProducer

**SinkPipeline Restart:**
Broadway handles gracefully - drains in-flight messages before shutdown.

### 7.3 Crash Recovery

**Scenario: SlotProducer crashes while processing transaction**

1. Transaction with LSN 100 has 10 messages (idx 0-9)
2. SlotProducer crashes after dispatching messages 0-5
3. Messages 0-5 are buffered in SlotMessageStores
4. On restart:
   - Fetch `restart_wal_cursor` = `{100, 0}` (minimum across stores)
   - Reconnect and replay from LSN 100
   - Skip messages 0-5 (guard: `below_restart_wal_cursor?/1`)
   - Dispatch messages 6-9

**Scenario: SlotMessageStore crashes**

1. Store crashes with messages in ETS and unflushed to DB
2. SlotProcessorServer detects crash (monitoring)
3. SlotProcessorServer crashes (monitored process died)
4. Supervision tree restarts entire SlotProducer pipeline
5. On restart:
   - SlotMessageStore loads persisted messages from DB
   - Missing messages (ETS only) are replayed from WAL
   - `restart_wal_cursor` ensures no data loss

### 7.4 Backpressure Mechanisms

**Level 1: ReorderBuffer → SlotProducer**
- If `ready_batches_by_idx` count exceeds threshold, stop asking for demand
- SlotProducer buffers messages in `accumulated_messages`
- If buffer fills, SlotProducer switches to `:buffering` state

**Level 2: SlotMessageStore → MessageHandler**
- If `payload_size_bytes > max_memory_bytes`, return error
- MessageHandler retries with exponential backoff
- Eventually blocks SlotProcessorServer's batch handling

**Level 3: PostgreSQL WAL Growth**
- If Sequin stops ACKing, WAL accumulates on disk
- PostgreSQL may run out of space (operator alert)

**System Health Checks:**
ReorderBuffer periodically checks:
- Available memory
- Disk space
- If unhealthy, stops pulling messages

---

## 8. PERFORMANCE & SCALABILITY

### 8.1 Partitioning Strategy

**Processor Partitions:**
- One partition per CPU core (typically 8-16)
- Messages are round-robin distributed by SlotProducer
- Parallel decoding and type casting
- ReorderBuffer reorders before delivery

**SlotMessageStore Partitions:**
- Configurable per consumer (default 1)
- Messages partitioned by `group_id` hash
- Enables parallel consumption for throughput
- Maintains ordering within each partition

**Partition Count Selection:**
- Single partition: Full ordering, lower throughput
- Multiple partitions: Parallel processing, ordering per partition
- Trade-off: Throughput vs. ordering guarantees

### 8.2 Memory Management

**SlotMessageStore Memory Limits:**
```elixir
max_memory_bytes = if system_max_memory_bytes do
  # Self-hosted: Divide total memory across all consumers
  system_max_memory_bytes / active_consumer_count
else
  # Cloud: Per-consumer limit from configuration
  consumer.max_storage_mb * 1_000_000
end
```

**Overflow to Database:**
When memory limit reached:
1. Sort ETS table by age (oldest first)
2. Flush oldest messages to PostgreSQL
3. Remove from ETS
4. Update `payload_size_bytes`

**Garbage Collection:**
Elixir processes explicitly call `:erlang.garbage_collect()` after removing large data structures (e.g., preloaded tables).

### 8.3 Connection Pooling

**Database Connections:**
Uses `Sequin.Databases.ConnectionCache` to pool connections:
- One pool per PostgreSQL database
- Configurable pool size (default 10)
- Connections are lazily created

**Sink Connections:**
- Kafka: `Sequin.Sinks.Kafka.ConnectionCache`
- Redis: `Sequin.Sinks.Redis.ConnectionCache`
- RabbitMQ: `Sequin.Sinks.RabbitMq.ConnectionCache`
- NATS: `Sequin.Sinks.Nats.ConnectionCache`

Each maintains a GenServer-based cache of connections keyed by configuration hash.

### 8.4 Batch Processing

**SlotProducer Batching:**
- Configurable `batch_flush_interval` (default 1 second)
- Accumulates messages until interval expires
- Emits batch marker
- Reduces batch overhead

**SinkPipeline Batching:**
- Broadway batchers group messages by routing key
- Configurable batch size and timeout
- Reduces API calls (e.g., 1 Kafka produce call for 100 messages)

**TableReaderServer Batching:**
- Page size dynamically adjusted by `PageSizeOptimizer`
- Starts small, increases if fast
- Decreases if timeouts occur
- Target: 1-2 second per page

---

## 9. MONITORING & METRICS

### 9.1 Health Events

**File:** `sequin/lib/sequin/health.ex`

Health events are emitted for:
- `:replication_connected` - SlotProducer connected
- `:replication_heartbeat_verification` - Heartbeat health check
- `:messages_ingested` - Messages written to SlotMessageStore
- `:messages_pending_delivery` - Messages available for delivery
- `:messages_filtered` - Filter function evaluation

**Storage:**
Health events are stored in `health_snapshots` table and can trigger alerts.

### 9.2 Prometheus Metrics

**File:** `sequin/lib/sequin/prometheus.ex`

Key metrics:
- `sequin_messages_ingested_total` - Messages written to stores
- `sequin_message_deliver_attempt_total` - Delivery attempts
- `sequin_message_deliver_success_total` - Successful deliveries
- `sequin_message_deliver_failure_total` - Failed deliveries
- `sequin_delivery_latency_microseconds` - Time to deliver
- `sequin_internal_latency_microseconds` - Sequin processing time
- `sequin_ingestion_latency_microseconds` - End-to-end from WAL
- `sequin_slot_producer_raw_bytes_received_total` - WAL bytes
- `sequin_slot_processor_server_busy_percent` - CPU utilization

### 9.3 Process Metrics

**File:** `sequin/lib/sequin/process_metrics.ex`

Each key process tracks:
- Message throughput (per second)
- Busy percentage (time spent processing vs. idle)
- Operation timing breakdown
- Memory usage

Logged periodically and exposed via Prometheus.

### 9.4 Tracing

**File:** `sequin/lib/sequin/runtime/trace.ex`

Trace events capture message flow through the system:
```elixir
Trace.info(consumer_id, %Trace.Event{
  message: "Delivered messages",
  extra: %{messages: [...]}
})
```

Used for debugging and audit trails.

---

## 10. COMMON FAILURE MODES & DEBUGGING

**iex required:** The Elixir snippets below must be run inside an **iex** (Interactive Elixir) session. iex is Elixir's interactive shell where you can run Elixir expressions against a live running node. See the [official iex docs](https://hexdocs.pm/iex/IEx.html) for more. To connect to the running Sequin node, run:
```
./sequin-iex.sh
```
Once connected, you can paste the commands directly.

**Tip — live node inspection with `:observer_cli`:** Once inside iex, you can run:
```elixir
:observer_cli.start()
```
This launches a terminal-based dashboard (no GUI needed) that gives you a real-time view of the running Sequin node: all active processes, memory consumption, CPU usage, message queue lengths, ETS table sizes, and more. It is the fastest way to spot a stuck or overloaded process, a memory leak, or an unexpectedly large ETS table without having to know the exact process name upfront. Use arrow keys to navigate, `q` to quit.

**PostgreSQL user permissions:** Querying replication slot info can be done by any user:
```sql
SELECT slot_name, confirmed_flush_lsn, restart_lsn, active FROM pg_replication_slots;
```
However, **dropping or recreating a slot must be run as the admin user**:
```sql
-- Drop a slot (admin only)
SELECT pg_drop_replication_slot('slot_name');

-- Recreate a slot (admin only)
SELECT pg_create_logical_replication_slot('slot_name', 'pgoutput');
```

### 10.1 WAL Growth (Disk Space)

**Symptom:** PostgreSQL disk usage grows unbounded

**Cause:** Sequin not ACKing WAL (slot not advancing)

**Debug:**
```sql
SELECT slot_name, confirmed_flush_lsn, restart_lsn 
FROM pg_replication_slots;
```

**Common Reasons:**
1. SlotProducer disconnected - Check health events
2. SlotMessageStore full - Check memory metrics
3. Consumer stuck - Check SinkPipeline metrics

### 10.2 Message Loss

**Symptom:** Messages not delivered, no errors

**Debug:**
1. Check if message matched consumer filters:
   ```elixir
   Consumers.matches_message?(consumer, message)
   ```
2. Check SlotMessageStore buffer:
   ```elixir
   :ets.info(SlotMessageStore.ets_table_name(consumer_id, partition))
   ```
3. Check MessageLedgers:
   ```elixir
   MessageLedgers.filter_delivered_wal_cursors(consumer_id, [wal_cursor])
   # If returned, it was already delivered
   ```

### 10.3 Duplicate Messages

**Symptom:** Same message delivered twice

**Cause:** Idempotency not working

**Debug:**
1. Check Redis delivered cursors set:
   ```
   ZCARD message_ledgers:delivered_cursors:{consumer_id}
   ```
2. Check if consumer is marking messages as delivered
3. Check if `MessageLedgers.trim_delivered_cursors_set/2` is running

### 10.4 Heartbeat Verification Failures

**Symptom:** SlotProcessorServer crashes with `:heartbeat_verification_failed`

**Cause:** Replication connection stale

**Debug:**
1. Check PostgreSQL logs for connection errors
2. Check network latency to PostgreSQL
3. Check if heartbeat table exists:
   ```sql
   SELECT * FROM public.sequin_logical_messages;
   ```
4. Check slot status:
   ```sql
   SELECT * FROM pg_replication_slots WHERE slot_name = '...';
   ```

### 10.5 Backfill Not Completing

**Symptom:** Backfill stuck in `:active` state

**Debug:**
1. Check TableReaderServer state:
   ```elixir
   :sys.get_state(TableReaderServer.via_tuple(backfill_id))
   ```
2. Check for watermark messages in logs
3. Check if SlotProcessorServer is receiving logical messages
4. Verify `TableReader.flush_batch/2` is being called

### 10.6 Monitor Ref Mismatch

**Symptom:** Error in `SlotProcessorServer.restart_wal_cursor!/1` about mismatched sink consumers

**Cause:** Consumer was added/removed but monitor refs not updated

**Fix:** Restart SlotProducer pipeline:
```elixir
Sequin.Runtime.Supervisor.restart_replication(slot_supervisor(), pg_replication)
```

---

## 11. KEY DATABASE TABLES

### 11.1 Configuration Schema Tables

**postgres_replication_slots:**
```sql
CREATE TABLE postgres_replication_slots (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL,
  postgres_database_id UUID NOT NULL,
  slot_name TEXT NOT NULL,
  publication_name TEXT NOT NULL,
  status TEXT NOT NULL, -- 'active' or 'disabled'
  annotations JSONB DEFAULT '{}',
  partition_count INTEGER DEFAULT 1,
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  UNIQUE (slot_name, postgres_database_id)
);
```

**sink_consumers:**
```sql
CREATE TABLE sink_consumers (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL,
  replication_slot_id UUID NOT NULL,
  name TEXT NOT NULL,
  type TEXT NOT NULL, -- 'http_push', 'kafka', 'sqs', etc.
  status TEXT NOT NULL, -- 'active', 'disabled', 'paused'
  message_kind TEXT NOT NULL, -- 'event' or 'record'
  partition_count INTEGER DEFAULT 1,
  max_storage_mb INTEGER,
  -- Sink-specific config (polymorphic)
  ...
);
```

**backfills:**
```sql
CREATE TABLE backfills (
  id UUID PRIMARY KEY,
  sink_consumer_id UUID NOT NULL,
  table_oid INTEGER NOT NULL,
  state TEXT NOT NULL, -- 'active', 'completed', 'cancelled'
  cursor JSONB, -- Keyset cursor for pagination
  max_timeout_ms INTEGER,
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
```

### 11.2 Stream Schema Tables

**consumer_events:**
```sql
CREATE TABLE consumer_events (
  consumer_id UUID NOT NULL,
  id BIGSERIAL,
  commit_lsn BIGINT NOT NULL,
  commit_idx INTEGER NOT NULL,
  table_oid INTEGER NOT NULL,
  record_pks TEXT[] NOT NULL,
  group_id TEXT,
  state TEXT NOT NULL, -- 'available', 'delivered'
  ack_id UUID NOT NULL,
  deliver_count INTEGER DEFAULT 0,
  last_delivered_at TIMESTAMP,
  not_visible_until TIMESTAMP,
  data JSONB NOT NULL,
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (consumer_id, id),
  INDEX (consumer_id, state) WHERE state = 'available',
  INDEX (consumer_id, not_visible_until) WHERE state = 'available'
);
```

**consumer_records:**
```sql
CREATE TABLE consumer_records (
  consumer_id UUID NOT NULL,
  id BIGSERIAL,
  commit_lsn BIGINT NOT NULL,
  commit_idx INTEGER NOT NULL,
  table_oid INTEGER NOT NULL,
  record_pks TEXT[] NOT NULL,
  group_id TEXT NOT NULL,
  state TEXT NOT NULL, -- 'available', 'delivered', 'acked', 'pending_redelivery'
  ack_id UUID NOT NULL,
  deliver_count INTEGER DEFAULT 0,
  last_delivered_at TIMESTAMP,
  not_visible_until TIMESTAMP,
  data JSONB NOT NULL,
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (consumer_id, id),
  INDEX (consumer_id, state, group_id),
  INDEX (consumer_id, not_visible_until)
);
```

**wal_events:**
```sql
CREATE TABLE wal_events (
  id BIGSERIAL PRIMARY KEY,
  wal_pipeline_id UUID NOT NULL,
  commit_lsn BIGINT NOT NULL,
  commit_idx INTEGER NOT NULL,
  source_table_oid INTEGER NOT NULL,
  source_table_schema TEXT,
  source_table_name TEXT,
  record_pks TEXT[] NOT NULL,
  record JSONB NOT NULL,
  changes JSONB,
  action TEXT NOT NULL, -- 'insert', 'update', 'delete'
  committed_at TIMESTAMP NOT NULL,
  transaction_annotations JSONB,
  inserted_at TIMESTAMP NOT NULL,
  INDEX (wal_pipeline_id),
  INDEX (commit_lsn)
);
```

---

## 12. CONFIGURATION

### 12.1 Application Config

**File:** `sequin/config/config.exs`

Key configurations:
```elixir
config :sequin, Sequin.Runtime.SlotProducer,
  batch_flush_interval: [
    default: 1000,  # 1 second
    min: 100,
    max: 60000
  ],
  ack_interval: 5000,  # 5 seconds
  reconnect_interval: 10000  # 10 seconds

config :sequin, Sequin.Runtime.SlotProducer.ReorderBuffer,
  max_demand: 1000,
  min_demand: 500,
  retry_flush_batch_interval: 1000

config :sequin, :max_memory_bytes, 1_000_000_000  # 1GB (self-hosted)

config :sequin, :message_handler_module, Sequin.Runtime.MessageHandler
```

### 12.2 Consumer Configuration

Stored in `sink_consumers` table, loaded into `%SinkConsumer{}` struct:

```elixir
%SinkConsumer{
  name: "my-consumer",
  type: :http_push | :kafka | :sqs | ...,
  message_kind: :event | :record,
  partition_count: 1,
  max_storage_mb: 100,
  
  # Filtering
  filter: %Function{code: "event.record.age > 18"},
  schema_filter: ["public"],
  
  # Transformation
  enrichment: %Function{code: "merge(event, {extra: 'data'})"},
  routing: %Function{code: "{topic: event.table}"},
  
  # Source
  source: %Source{table_oids: [16384, 16385]},
  
  # Sink-specific (polymorphic)
  http_endpoint: %HttpEndpoint{url: "https://..."},
  kafka_sink: %KafkaSink{hosts: "...", topic: "..."},
  ...
}
```

---

## 13. DEPLOYMENT & OPERATIONS

### 13.1 Starting Sequin

1. **Database Migration:**
   ```bash
   mix ecto.migrate
   ```

2. **Start Application:**
   ```bash
   mix phx.server
   # Or in production:
   _build/prod/rel/sequin/bin/sequin start
   ```

3. **Create Replication Slot:**
   Via API or web UI, which:
   - Creates publication in PostgreSQL
   - Creates replication slot
   - Starts SlotProducer pipeline

### 13.2 Monitoring Checklist

- [ ] PostgreSQL disk space (WAL growth)
- [ ] Sequin memory usage (SlotMessageStore buffers)
- [ ] Replication lag (confirmed_flush_lsn vs. current LSN)
- [ ] Consumer delivery rates (Prometheus metrics)
- [ ] Health event failures
- [ ] Redis connection health

### 13.3 Common Operations

**Pause Consumer:**
```elixir
Consumers.update_sink_consumer(consumer, %{status: :paused})
# Stops SinkPipeline, keeps buffering messages
```

**Resume Consumer:**
```elixir
Consumers.update_sink_consumer(consumer, %{status: :active})
Sequin.Runtime.Supervisor.start_for_sink_consumer(consumer)
```

**Reset Consumer (clear buffer):**
```elixir
SlotMessageStore.discard_all_messages(consumer)
```

**Backfill Table:**
```elixir
Consumers.create_backfill(%{
  sink_consumer_id: consumer.id,
  table_oid: 16384,
  state: :active
})
# TableReaderServer will start automatically
```

---

## 14. TESTING

### 14.1 Test Support

**File:** `sequin/test/support/`

Key modules:
- `AccountsSupport` - Create test accounts
- `DatabasesSupport` - Create test PostgreSQL databases
- `ConsumersSupport` - Create test consumers
- `ReplicationSupport` - Create test replication slots

**Sandbox Mode:**
Tests use `Ecto.Adapters.SQL.Sandbox` to isolate database transactions.

### 14.2 Test Patterns

**Mocking:**
Uses `Mox` library for behavior mocks:
- `Sequin.Runtime.MessageHandlerMock`
- `Sequin.Runtime.SlotMessageStoreMock`
- `Sequin.TestSupport.DateTimeMock`

**Process Testing:**
```elixir
test "SlotProducer emits messages" do
  # Start SlotProducer with test_pid
  {:ok, _} = SlotProducer.start_link(test_pid: self(), ...)
  
  # Assert messages received
  assert_receive {SlotProcessorServer, :flush_messages, count}
end
```

---

## 15. TROUBLESHOOTING GUIDE

**iex:** The Elixir snippets below must be run inside an **iex** (Interactive Elixir) session. iex is Elixir's interactive shell — think of it like a Python REPL or a Node.js console — where you can run Elixir expressions against a live running node. See the [official iex docs](https://hexdocs.pm/iex/IEx.html) for more. To connect to the running Sequin node, run:
```
./sequin/sequin-iex.sh
```
Once connected, you can paste the commands directly.

### 15.1 No Messages Flowing

**Check:**
1. Is replication slot created in PostgreSQL?
   ```sql
   SELECT * FROM pg_replication_slots;
   ```
2. Is publication created?
   ```sql
   SELECT * FROM pg_publication;
   SELECT * FROM pg_publication_tables;
   ```
3. Is SlotProducer connected?
   - Check health events for `:replication_connected`
4. Are there matching consumers?
   ```elixir
   Consumers.list_sink_consumers() |> Enum.filter(&(&1.status == :active))
   ```

### 15.2 Messages Stuck in Buffer

**Check:**
1. SlotMessageStore payload size:
   ```elixir
   state = :sys.get_state(SlotMessageStore.via_tuple(consumer_id, 0))
   state.payload_size_bytes
   ```
2. Is SinkPipeline running?
   ```elixir
   Process.whereis(SinkPipeline.via_tuple(consumer_id))
   ```
3. Check sink health (HTTP endpoint reachable, Kafka cluster up, etc.)

### 15.3 High Latency

**Check:**
1. Ingestion latency (Prometheus):
   - `sequin_ingestion_latency_microseconds`
2. Internal latency:
   - `sequin_internal_latency_microseconds`
3. Delivery latency:
   - `sequin_delivery_latency_microseconds`
4. Process busy percentages:
   - `sequin_slot_processor_server_busy_percent`

**Common Causes:**
- Slow sink (HTTP endpoint, Kafka cluster)
- Large payload sizes (TOAST columns)
- Too many consumers on one slot
- Undersized partition counts

---

## 16. ADVANCED TOPICS

### 16.1 Transaction Annotations

Custom metadata can be added to transactions:
```sql
SELECT pg_logical_emit_message(true, 'sequin:transaction_annotations.set', 
  '{"user_id": "123", "request_id": "abc"}');
-- Make changes
SELECT pg_logical_emit_message(true, 'sequin:transaction_annotations.clear', '');
```

Annotations flow through:
1. SlotProducer captures in transaction state
2. Attached to all messages in transaction
3. Available in consumer data

### 16.2 Message Partitioning

Consumer partitions enable parallel processing:
```elixir
# Set partition count
consumer = Consumers.update_sink_consumer(consumer, %{partition_count: 4})

# Messages are partitioned by group_id hash
partition = :erlang.phash2(message.group_id, 4)
```

**group_id** is derived from:
- Record PKs for record consumers
- Configurable function for event consumers

### 16.3 Enrichment & Filtering

**Filter Function:**
```javascript
// Only process users over 18
event.record.age > 18
```

**Enrichment Function:**
```javascript
// Add computed field
merge(event, {
  full_name: event.record.first_name + " " + event.record.last_name
})
```

**Routing Function:**
```javascript
// Dynamic Kafka topic
{topic: "user_" + event.record.country}
```

Functions run in `Sequin.Functions.MiniElixir` (sandboxed JavaScript-like language).

---

## 17. FILE REFERENCE

### Core Runtime Components
- `sequin/lib/sequin/runtime/supervisor.ex` - Top-level runtime supervisor
- `sequin/lib/sequin/runtime/slot_supervisor.ex` - Per-slot supervisor
- `sequin/lib/sequin/runtime/slot_producer/slot_producer.ex` - WAL stream producer
- `sequin/lib/sequin/runtime/slot_producer/supervisor.ex` - SlotProducer pipeline supervisor
- `sequin/lib/sequin/runtime/slot_producer/processor.ex` - Message decoder
- `sequin/lib/sequin/runtime/slot_producer/reorder_buffer.ex` - Message reorderer
- `sequin/lib/sequin/runtime/slot_processor_server.ex` - Batch coordinator
- `sequin/lib/sequin/runtime/message_handler.ex` - Message router
- `sequin/lib/sequin/runtime/slot_message_store.ex` - Per-consumer buffer
- `sequin/lib/sequin/runtime/consumer_producer.ex` - Broadway producer
- `sequin/lib/sequin/runtime/sink_pipeline.ex` - Broadway pipeline
- `sequin/lib/sequin/runtime/table_reader_server.ex` - Backfill reader

### Data Models
- `sequin/lib/sequin/replication/postgres_replication_slot.ex`
- `sequin/lib/sequin/consumers/sink_consumer.ex`
- `sequin/lib/sequin/consumers/consumer_event.ex`
- `sequin/lib/sequin/consumers/consumer_record.ex`
- `sequin/lib/sequin/replication/wal_event.ex`

### Sink Implementations
- `sequin/lib/sequin/runtime/http_push_pipeline.ex`
- `sequin/lib/sequin/runtime/kafka_pipeline.ex`
- `sequin/lib/sequin/runtime/sqs_pipeline.ex`
- `sequin/lib/sequin/runtime/redis_stream_pipeline.ex`

### Supporting Modules
- `sequin/lib/sequin/runtime/message_ledgers.ex` - Idempotency & ALO tracking
- `sequin/lib/sequin/postgres.ex` - PostgreSQL utilities
- `sequin/lib/sequin/health.ex` - Health event system
- `sequin/lib/sequin/prometheus.ex` - Metrics

---

## 18. GLOSSARY

- **ACK (Acknowledge):** Confirm message delivery/processing
- **ALO (At-Least-Once):** Delivery guarantee (message delivered ≥1 time)
- **Backfill:** Reading historical table data
- **Batch Marker:** Signal that a batch is complete
- **CDC (Change Data Capture):** Capturing database changes
- **Commit Index (commit_idx):** Position within a transaction
- **Consumer:** Destination for messages (HTTP, Kafka, etc.)
- **ETS (Erlang Term Storage):** In-memory key-value store
- **GenServer:** Elixir process abstraction
- **GenStage:** Elixir streaming abstraction (producer/consumer)
- **LSN (Log Sequence Number):** PostgreSQL WAL position
- **Partition:** Parallel processing unit
- **Publication:** PostgreSQL configuration for logical replication
- **Replication Slot:** PostgreSQL replication tracking mechanism
- **Sink:** External system receiving messages
- **TOAST (The Oversized-Attribute Storage Technique):** PostgreSQL large value storage
- **WAL (Write-Ahead Log):** PostgreSQL transaction log
- **WAL Cursor:** Position identifier (LSN + index)

---

This documentation provides a comprehensive overview of the Sequin architecture suitable for maintaining the system. For specific implementation details, refer to the source files listed in the File Reference section.