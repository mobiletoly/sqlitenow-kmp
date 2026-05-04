---
layout: page
title: Flutter/Dart
permalink: /flutter/
---

# Flutter/Dart

This path is for Flutter apps and pure Dart packages. It uses the Dart CLI and
runtime packages directly. It does not require the SQLiteNow Gradle plugin,
Kotlin source sets, or a manual compiler jar path in normal release consumption.

## Start Here

The getting-started guide is a complete walkthrough: install the packages,
create SQL files, generate Dart code, open a database, use adapters, execute
queries, watch changes, and test the flow.

[Getting started]({{ site.baseurl }}/flutter/getting-started/)

## SQL Reference

Reference topics use the same SQL concepts as KMP docs, but render Dart paths,
generated Dart APIs, and Dart runtime examples.

[SQL reference]({{ site.baseurl }}/flutter/sql-reference/)

## Recipes

Recipes cover common implementation patterns such as adapters, reactive
watchers, parameter inference, and nested result mapping.

[Recipes]({{ site.baseurl }}/flutter/recipes/)

## Runtime Reference

The Dart runtime covers database open and close, migrations, typed query runners,
execute statements, transactions, invalidation, external table change reporting,
and adapters.

[Runtime guide]({{ site.baseurl }}/flutter/runtime/)

## Migrations

Migration files define how existing app databases move between schema versions.
Fresh databases are bootstrapped from the current schema, while existing
databases run versioned upgrade SQL.

[Migration guide]({{ site.baseurl }}/flutter/migrations/)

## Example

The repository contains a Flutter todo example using SQL assets, generated Dart
code, custom adapters, transactions, and watch/invalidation.

[Example walkthrough]({{ site.baseurl }}/flutter/example/)

## Web Status

Dart web support is intentionally kept behind the runtime driver boundary. The
public Dart runtime currently targets Dart VM and Flutter native runtimes through
`package:sqlite3`.
