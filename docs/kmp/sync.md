---
layout: page
title: KMP Sync
permalink: /kmp/sync/
---

# KMP Sync

Oversqlite is the SQLiteNow sync runtime for Kotlin Multiplatform apps. The KMP
docs cover generated bridge setup, local lifecycle, account attachment, sync
operations, reactive updates, and recovery.

## Start Here

Follow the KMP setup guide when enabling sync in a Gradle-managed SQLiteNow
database:

[Getting Started]({{ site.baseurl }}/kmp/sync/getting-started/)

## Lifecycle

Use the lifecycle guide to separate local startup, authenticated attachment, and
explicit rebuild recovery:

[Open, Attach & Rebuild]({{ site.baseurl }}/kmp/sync/open-connect-rebuild/)

## Operations

Use the operations reference for `pushPending()`, `pullToStable()`, `sync()`,
`syncThenDetach()`, automatic downloads, watch wake-ups, and rebuild result
types:

[Sync Operations]({{ site.baseurl }}/kmp/sync/sync-operations/)

## Reactive Updates

Generated KMP flows are invalidated when oversqlite applies managed table
changes:

[Reactive Updates]({{ site.baseurl }}/kmp/sync/reactive-updates/)

## Shared Concepts And Server Setup

The protocol concepts and server responsibilities are shared across KMP,
Flutter/Dart, and Swift clients:

- [Core Concepts]({{ site.baseurl }}/sync/core-concepts/)
- [Server Setup]({{ site.baseurl }}/sync/server-setup/)
