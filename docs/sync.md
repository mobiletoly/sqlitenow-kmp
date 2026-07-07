---
layout: page
title: Sync
permalink: /sync/
---

# Sync

SQLiteNow sync is built around oversqlite clients that talk to an oversync
server. Client setup is framework-specific; protocol concepts and server
responsibilities are shared.

## Framework Client Guides

- [KMP Sync]({{ site.baseurl }}/kmp/sync/)
- [Flutter/Dart Sync]({{ site.baseurl }}/flutter/sync/)
- [Swift Sync]({{ site.baseurl }}/swift/sync/)

## Shared Concepts

Start with the shared lifecycle vocabulary before wiring a client:

[Core Concepts]({{ site.baseurl }}/sync/core-concepts/)

## Server Setup

Use the server guide for endpoint responsibilities, authentication boundaries,
watch wake-ups, and authoritative PostgreSQL-backed storage:

[Server Setup]({{ site.baseurl }}/sync/server-setup/)
