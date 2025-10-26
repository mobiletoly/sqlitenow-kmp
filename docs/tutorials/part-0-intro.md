---
layout: doc
title: Part 0 – Series Overview
permalink: /tutorials/part-0-intro/
parent: Tutorials
nav_order: 0
---

# SQLiteNow Mood Tracker Tutorial Series

Welcome aboard! This series is a hands-on introduction to **SQLiteNow**, our Kotlin
Multiplatform persistence stack that keeps SQL at the center of your workflow. Unlike
annotation-heavy ORMs such as Room, SQLiteNow is proudly SQL-first: you write plain `.sql`
files, keep using the editor you already love, and let the Gradle plugin generate type-safe
Kotlin around your queries—no IDE extensions required.

## What Is SQLiteNow?

SQLiteNow couples a Gradle code generator with a multiplatform runtime. Point it at your
schema, migrations, and queries; the Gradle plugin validates them at build time and emits Kotlin
sources that:

- expose strongly typed routers (generated query objects you call from Kotlin—no hand-written DAOs)
  for every statement,
- map results into data classes, collections, and nested aggregates,
- plug in adapters for [enums]({{ site.baseurl }}/recipes/serialization/),
  custom serializers, and complex joins,
- run the exact same API on Android/JVM, iOS/Native, desktop, JS, and wasm.

Use inline `@@{ ... }` comment-style annotations to embed nested objects and group child rows into
collections—see [Dynamic Fields]({{ site.baseurl }}/documentation/dynamic-fields/).

Because everything happens at compile time, there are no runtime reflection penalties or
annotation processors to wrestle with. The runtime handles connection management, migrations,
reactive flows, and per-platform persistence (IndexedDB snapshots on web, `:memory:` toggles
for tests, bundled drivers elsewhere).

### How is SQLiteNow different?

**Versus Room (Jetpack, KMP):** Room centers on annotated entities/DAOs and provides an abstraction
layer over SQLite. SQLiteNow is SQL‑first and multiplatform (Android, iOS, JVM/desktop, JS, Wasm).
You write `.sql` files; the Gradle plugin validates them at build time and generates type‑safe
Kotlin—no entities, no DAOs, no runtime annotations.

**Versus SQLDelight:** Both are SQL‑first. SQLiteNow uses inline comment annotations (`-- @@{ … }`)
to shape results—including nested objects and grouped collections—without custom mappers; it doesn't
rely on an IDE plugin, and it includes an optional multi‑device sync module. If you like SQLDelight,
you'll feel at home, but SQLiteNow leans into SQLite‑only optimizations and allows generating
dynamic result mapping. [Dynamic Fields]({{ site.baseurl }}/documentation/dynamic-fields/) · [Serialization]({{ site.baseurl }}/recipes/serialization/)

## Why This Series Matters

We want you to see how approachable SQL-first development can be. To do that we will build a
compact **Mood Tracker** application that still showcases why SQLiteNow stands out:

- define schema assets once and generate multiplatform Kotlin code,
- rely on typed parameters for inserts, selects, and reactive query flows,
- run the same migrations and storage strategies on every target,
- keep validation tight with Android instrumentation tests.

Even if you are used to Room or other ORMs, you will experience how staying close to SQL
gives you more control with less ceremony.

## Series Roadmap

The tutorial unfolds in three articles (plus this introduction) so you can follow at your own pace.

1. **Bootstrapping** – hook in the Gradle plugin, define schema and queries, and call the
   generated API from shared code,
2. **Tags, Filters, and Richer Types** – extend the schema with tags, show how SQLiteNow handles
   relationships, and lean on column-level annotations to surface Kotlin types such as `Uuid` and
   `Int` without manual casts,
3. **Reactive Mood Dashboard** – promote the generated `selectRecent` query to a shared
   `StateFlow`, compute a "this week" mood summary that recomputes whenever data changes, build a
   Compose screen with a quick-add form, the weekly summary card, and a reactive list, ensure the
   Android build stores the database under app-private storage, and round the weekly average to a
   single decimal.

Each part lives in its own Markdown file so you can read, copy snippets, and line it up with
the source under version control. We will also add instrumentation tests alongside the
implementation to keep everything verifiable.

## Who Should Follow Along

This series is for Kotlin developers who want SQL-level control without the tooling
overhead. If you have felt boxed in by annotation-driven ORMs, or if you need one
persistence story that spans Android, iOS, desktop, and web, SQLiteNow was built for you.

Ready to see SQLiteNow in action? Jump into [Part 1]({{ site.baseurl }}/tutorials/part-1-bootstrap/) to wire up the essentials.
