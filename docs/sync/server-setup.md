---
layout: doc
title: Server Setup
permalink: /sync/server-setup/
parent: Sync
parent_url: /sync/
---

# Server Setup

To enable synchronization in your SQLiteNow application, you need a sync server that handles data
coordination between devices. We provide **go-oversync**, a production-ready sync server
implementation.

## Why You Need a Sync Server

SQLiteNow's sync system follows a **server-coordinated** architecture where:

- **Clients upload** their local changes to the server
- **Server assigns** global sequence numbers and resolves conflicts
- **Clients download** changes from other devices via the server
- **Server maintains** the authoritative timeline of all changes

This approach ensures:
- **Consistent ordering** of changes across all devices
- **Reliable conflict resolution** using server-side logic
- **Scalable architecture** that works with any number of devices
- **Offline-first experience** with eventual consistency

## go-oversync: Production-Ready Sync Server

**go-oversync** is our official sync server implementation, written in Go. It provides:

### Key Features

- **Automatic Change Tracking** - Tracks all INSERT, UPDATE, DELETE operations on sync-enabled tables
- **Conflict Resolution** - Optimistic concurrency control with automatic conflict detection
- **Offline-First** - Works seamlessly offline, syncs when connection is available
- **Secure Authentication** - Pluggable authentication with any system (JWT, sessions, API keys)
- **Incremental Sync** - Efficient sync with pagination and change-based updates
- **Foreign Key Aware** - Respects database relationships and ordering constraints

### Architecture Benefits
- **Stateless Design**: Each request is independent, enabling load balancing
- **Database-Backed**: All sync state persists in PostgreSQL for reliability
- **Conflict Resolution**: Server-side logic ensures consistent data across devices

## Getting Started

The complete setup guide, deployment instructions, and API documentation are available in the
go-oversync repository:

**🔗 [go-oversync on GitHub](https://github.com/mobiletoly/go-oversync)**


## Alternative Server Implementations

While go-oversync is our recommended solution, the SQLiteNow sync protocol is open and
well-documented. You can implement your own sync server in any language by following the protocol
specification.

The sync protocol uses standard HTTP/JSON and is designed to be simple to implement while remaining
powerful and scalable.

## Capabilities Table Contract

Every successful `GET /sync/capabilities` response must include `registered_table_specs`. Each item
must contain a non-blank `schema`, a non-blank `table`, and `sync_key_columns` with exactly one
non-blank entry. Duplicate table specs are invalid. `registered_tables` may remain as an optional
legacy summary, but clients use `registered_table_specs` as canonical.

KMP and Dart clients compare this metadata with their validated generated configuration on every
capabilities request. Ordering of table specs is irrelevant; schema/table identifiers and ordered
key-column lists are exact. A mismatch fails before connect, outbox freezing, data download, or
snapshot creation. This contract is table/key compatibility checking only—it is not wire-profile
or projection negotiation.

Deploy server support for the required metadata before rolling out strict clients if older server
versions may still be live.

## Bundle-Change Watch

Bundle-change watch is optional server support for lower-latency automatic downloads. It does not
replace the normal pull/download endpoints.

A compatible server should:

- advertise support from `GET /sync/capabilities` with `features.bundle_change_watch = true`
- accept `GET /sync/watch?after_bundle_seq=...` for an authenticated, connected source
- keep the watch response open as a server-sent events stream
- emit bundle metadata events when newer committed bundles are available for the client
- optionally send heartbeat comments to keep intermediaries from closing an idle stream

Watch events are wake-up hints only. They do not include authoritative row payloads, and clients do
not apply watch event bodies to SQLite. After a watch event, clients still download and apply remote
data through the ordinary `pullToStable()` path. If watch is unsupported, disconnected, malformed,
or unavailable, SQLiteNow clients fall back to polling.

If your server emits absolute timestamps in sync payloads, use RFC3339/ISO-8601 strings with an
explicit zone such as `2026-03-24T18:42:11Z` or `2026-03-24T20:42:11+02:00`. Do not rely on naive
local timestamp text such as `2026-03-24 18:42:11` if the value represents a real instant. On the
client side, Oversqlite only gives special replay-equivalence handling to valid RFC3339 instants;
naive timestamp text remains ordinary opaque payload text.

---

**Next Steps**: Visit the [go-oversync repository](https://github.com/mobiletoly/go-oversync) to set
up your sync server and start synchronizing data across devices.
