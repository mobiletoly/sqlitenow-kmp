# Migrations

Migration files move existing databases from one schema version to the next.
The `schema/` directory always contains the latest full schema, while
`migration/` contains incremental upgrade SQL for databases already installed
on a device.

Generated databases also accept one optional `onMigrationStep` callback for
application-defined row transformations. SQLiteNow invokes it after each
integer version's SQL, including versions with no SQL file. A migration file
does not replace the callback for that version. For an existing database at
version 1, SQLiteNow runs `0002.sql`, calls the callback for `1 -> 2`, writes
`PRAGMA user_version = 2`, and commits.

Migration SQL, callback SQL, and the final version write share one transaction.
An unhandled callback failure rolls back all three. Fresh database creation and
reopening at the target version skip the callback. A database newer than the
generated target also runs no migration work and keeps its existing version.
SQLiteNow drains scoped operations accepted by a callback before it advances to
the next version. Cancellation during that drain cancels the accepted
operations and rolls back. Errors caught by the callback remain handled, so the
callback may run compensating SQL and commit.

{% if include.platform == "dart" %}
For the full Dart migration guide, see
[Flutter/Dart Migrations]({{ site.baseurl }}/flutter/migrations/).
{% elsif include.platform == "kmp" %}
For the full KMP migration guide, see
[KMP Migrations]({{ site.baseurl }}/kmp/migrations/).
{% elsif include.platform == "swift" %}
For the full Swift migration guide, see
[Swift Migrations]({{ site.baseurl }}/swift/migrations/).
{% endif %}
