---
layout: doc
title: Server Setup
permalink: /sync/server-setup/
parent: Sync
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

---

**Next Steps**: Visit the [go-oversync repository](https://github.com/mobiletoly/go-oversync) to set
up your sync server and start synchronizing data across devices.
