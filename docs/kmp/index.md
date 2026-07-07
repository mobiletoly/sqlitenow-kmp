---
layout: page
title: Kotlin Multiplatform
permalink: /kmp/
---

# Kotlin Multiplatform

This path is for Kotlin Multiplatform apps using the SQLiteNow Gradle plugin and
KMP runtime libraries. It covers Android, iOS, JVM, macOS, Linux, JavaScript,
and Kotlin/Wasm targets.

## Start Here

The getting-started guide is a complete walkthrough: apply the Gradle plugin,
add the runtime dependency, configure a database, create SQL files, generate KMP
code, and wire platform SQLite drivers.

[Getting started]({{ site.baseurl }}/kmp/getting-started/)

## SQL Reference

Reference topics use the same SQL concepts as Flutter/Dart docs, but render KMP
paths, generated Kotlin APIs, and Gradle plugin examples.

[SQL reference]({{ site.baseurl }}/kmp/sql-reference/)

## Recipes

Recipes cover common implementation patterns such as adapters, reactive flows,
parameter inference, schema inspection, and nested result mapping.

[Recipes]({{ site.baseurl }}/kmp/recipes/)

## Migrations

Migration files define how existing app databases move between schema versions.
Fresh databases are bootstrapped from the current schema, while existing
databases run versioned upgrade SQL.

[Migration guide]({{ site.baseurl }}/kmp/migrations/)

## Sync

The KMP sync docs cover client setup, bootstrap, sync operations, reactive
updates, and server integration.

[Sync docs]({{ site.baseurl }}/kmp/sync/)

## Tutorials

The tutorial series walks through a Kotlin Multiplatform app from project setup
to generated SQL APIs and reactive UI.

[Tutorials]({{ site.baseurl }}/kmp/tutorials/)
