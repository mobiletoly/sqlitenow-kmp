---
layout: page
title: Sync
permalink: /sync/
---

# Multi-Device Synchronization

SQLiteNow includes **OverSqlite**, a complete synchronization system for building multi-device
applications. OverSqlite is the sync component of the SQLiteNow framework that enables seamless data
sharing across multiple devices with automatic conflict resolution, change tracking, and
offline-first capabilities.

## Overview

OverSqlite is a client library (in `dev.goquick.sqlitenow.oversqlite` package) that synchronizes
SQLite databases by connecting to a server that supports the oversync protocol. 
**go-oversync** is a project written in Go that provides an adapter library for data
synchronization over PostgreSQL databases, allowing developers to build their own HTTP server
with minimum effort to expose synchronization endpoints.

### Architecture

- **OverSqlite Client**: Embedded in your app, tracks changes and syncs with server
- **Oversync Protocol**: Standardized protocol for SQLite synchronization
- **go-oversync Library**: Adapter library that enables building HTTP servers with sync endpoints
- **Your HTTP Server**: Built using go-oversync library to handle sync requests
- **PostgreSQL Database**: Server-side storage for the authoritative dataset

This architecture is designed for real-world multi-device applications where users expect their data
to be available and consistent across all their devices. Whether it's a note-taking app, task
manager, or collaborative tool, OverSqlite handles the complex aspects of data synchronization
automatically.

### Key Features

- **Automatic Change Tracking** - Tracks all INSERT, UPDATE, DELETE operations on sync-enabled
  tables
- **Conflict Resolution** - Pluggable conflict resolution strategies (Server Wins, Client Wins,
  custom)
- **Offline-First** - Works seamlessly offline, syncs when connection is available
- **Secure Authentication** - JWT-based authentication with automatic token refresh
- **Incremental Sync** - Efficient sync with pagination and change-based updates
- **Selective Sync** - Choose which tables to sync with `enableSync=true` annotation

## How It Works

OverSqlite follows a simple but powerful client-server pattern:

1. **Enabl sync** on the tables you want to synchronize
2. **Connect to sync HTTP server**, developer's own server that uses go-oversync library to handle 
   sync requests and synchronize data with PostgreSQL tables
3. **Configure authentication** between your app and the sync server
4. **Bootstrap your device** to prepare for sync operations
5. **Sync regularly** to keep data consistent across devices

OverSqlite handles all the complex aspects automatically - change tracking, conflict resolution,
network failures, and data consistency. Your HTTP server, built with the go-oversync library,
manages the authoritative state and coordinates between all connected devices.

## Architecture Overview

OverSqlite follows a client-server architecture using the oversync protocol:

1. **OverSqlite Clients** track local changes and periodically sync with the server
2. **HTTP Server** (built with go-oversync library) maintains the authoritative state in PostgreSQL
   and resolves conflicts
3. **Changes** are tracked at the row level with timestamps and device attribution
4. **Conflicts** are resolved using pluggable strategies when the same data is modified on multiple
   devices

```
┌─────────────┐    oversync   ┌─────────────┐   PostgreSQL   ┌─────────────┐
│   Device A  │   protocol    │ Your HTTP   │   connection   │ PostgreSQL  │
│ OverSqlite  │ ────────────► │   Server    │ ◄────────────► │  Database   │
│   Client    │               │(go-oversync)│                │             │
└─────────────┘               └─────────────┘                └─────────────┘
       ▲                             │                             ▲
       │                             │                             │
       │                             ▼                             │
┌─────────────┐   oversync    ┌─────────────┐                      │
│   Device B  │   protocol    │ Conflict    │                      │
│ OverSqlite  │ ◄──────────── │ Resolution  │ ─────────────────────┘
│   Client    │               │ & Sync      │
└─────────────┘               └─────────────┘
```

This architecture ensures data consistency while allowing offline operation and handling network
interruptions gracefully. Your HTTP server, built using the go-oversync library, acts as the central
coordinator, storing all data in PostgreSQL and managing synchronization between all connected
OverSqlite clients.

## Core Concepts

Start here to understand the fundamental concepts of SQLiteNow's synchronization system.

### [Core Concepts →]({{ site.baseurl }}/sync/core-concepts/)

Learn about users, devices, bootstrap, hydration, and other essential sync concepts.

## Getting Started

Ready to implement sync? Follow our step-by-step guide.

### [Getting Started →]({{ site.baseurl }}/sync/getting-started/)

Quick start guide with step-by-step instructions to enable sync in your application.

### [Bootstrap & Hydration →]({{ site.baseurl }}/sync/bootstrap-hydration/)

Understand the two-phase process of setting up sync for new devices and initial data loading.

### [Sync Operations →]({{ site.baseurl }}/sync/sync-operations/)

Deep dive into upload and download operations, conflict resolution, and sync strategies.

### [Reactive Sync Updates →]({{ site.baseurl }}/sync/reactive-updates/)

Learn how SQLiteNow automatically updates your UI when sync operations modify data.

### [Authentication →]({{ site.baseurl }}/sync/authentication/)

Configure JWT authentication, token refresh, and secure communication with your sync server.

### [Server Setup →]({{ site.baseurl }}/sync/server-setup/)

Set up your sync server to handle client requests and manage data synchronization.

## Navigation

<div class="doc-nav-grid">
  <a href="{{ site.baseurl }}/sync/core-concepts/" class="doc-nav-card">
    <h3>🎯 Core Concepts</h3>
    <p>Users, devices, bootstrap, hydration, and fundamental sync principles</p>
  </a>

  <a href="{{ site.baseurl }}/sync/getting-started/" class="doc-nav-card">
    <h3>🚀 Getting Started</h3>
    <p>Quick start guide to enable sync in your application</p>
  </a>

  <a href="{{ site.baseurl }}/sync/bootstrap-hydration/" class="doc-nav-card">
    <h3>🔄 Bootstrap & Hydration</h3>
    <p>Setting up new devices and initial data loading</p>
  </a>

  <a href="{{ site.baseurl }}/sync/sync-operations/" class="doc-nav-card">
    <h3>⚡ Sync Operations</h3>
    <p>Upload, download, and conflict resolution strategies</p>
  </a>

  <a href="{{ site.baseurl }}/sync/reactive-updates/" class="doc-nav-card">
    <h3>🔄 Reactive Updates</h3>
    <p>Automatic UI updates when sync operations modify data</p>
  </a>

  <a href="{{ site.baseurl }}/sync/authentication/" class="doc-nav-card">
    <h3>🔐 Authentication</h3>
    <p>JWT tokens, refresh logic, and secure communication</p>
  </a>

  <a href="{{ site.baseurl }}/sync/server-setup/" class="doc-nav-card">
    <h3>🖥️ Server Setup</h3>
    <p>Configure your sync server and API endpoints</p>
  </a>
</div>


<style>
.doc-nav-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 20px;
  margin: 30px 0;
}

.doc-nav-card {
  border: 1px solid #d0d7de;
  border-radius: 8px;
  padding: 20px;
  text-decoration: none;
  color: inherit;
  transition: all 0.2s ease;
  background: #f6f8fa;
}

.doc-nav-card:hover {
  border-color: #0969da;
  box-shadow: 0 4px 12px rgba(9, 105, 218, 0.1);
  transform: translateY(-2px);
}

.doc-nav-card h3 {
  margin: 0 0 10px 0;
  color: #24292f;
  font-size: 18px;
}

.doc-nav-card p {
  margin: 0;
  color: #656d76;
  font-size: 14px;
  line-height: 1.5;
}

.doc-nav-card:hover h3 {
  color: #0969da;
}
</style>
