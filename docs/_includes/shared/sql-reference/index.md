# SQL Reference

Reference documentation for SQLiteNow SQL files, annotations, query generation,
and generated APIs.

{% assign base = "/flutter/sql-reference" %}
{% assign migrations = "/flutter/migrations" %}
{% if include.platform == "kmp" %}
{% assign base = "/kmp/sql-reference" %}
{% assign migrations = "/kmp/migrations" %}
{% elsif include.platform == "swift" %}
{% assign base = "/swift/sql-reference" %}
{% assign migrations = "/swift/migrations" %}
{% endif %}

## Core Concepts

### Schema Definition

Define tables, indexes, and views with SQL files.

[Create Schema]({{ site.baseurl }}{{ base }}/create-schema/)

### Querying Data

Write SELECT statements and use generated typed query runners.

[Query Data]({{ site.baseurl }}{{ base }}/query-data/)

### Managing Data

Write INSERT, UPDATE, and DELETE statements with typed parameters.

[Manage Data]({{ site.baseurl }}{{ base }}/manage-data/)

### Initializing Data

Seed a fresh database from `init/` SQL files.

[Initialize Data]({{ site.baseurl }}{{ base }}/initialize-data/)

### Migrations

Move installed databases between schema versions.

[Migrations]({{ site.baseurl }}{{ migrations }}/)

### Dynamic Fields

Map JOIN-heavy SQL into nested result models.

[Dynamic Fields]({{ site.baseurl }}{{ base }}/dynamic-fields/)
