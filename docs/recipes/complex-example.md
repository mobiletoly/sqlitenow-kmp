---
layout: doc
title: Complex Example
permalink: /recipes/complex-example/
parent: Recipes
---

# Complex Example

This guide walks through building a rich read model that stitches together several tables,
maps nested structures, and keeps generated code readable. The example is intentionally
compact, but it highlights the patterns we rely on for larger production schemas.

> New to dynamic fields? Start with the [Dynamic Fields guide]({{ site.baseurl }}/documentation/dynamic-fields/)
> for a full breakdown of `perRow`, `collection`, and `entity` mapping options.

## 1. Schema overview

We will compose a bundle-centric view that pulls together a provider, bundle metadata,
the packages inside that bundle, and each package's scheduled activities.

```sql
CREATE TABLE provider (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL
);

CREATE TABLE bundle (
    id           INTEGER PRIMARY KEY,
    provider_id  INTEGER NOT NULL,
    title        TEXT NOT NULL,
    status       TEXT NOT NULL,
    FOREIGN KEY (provider_id) REFERENCES provider(id)
);

CREATE TABLE package (
    id         INTEGER PRIMARY KEY,
    bundle_id  INTEGER NOT NULL,
    title      TEXT NOT NULL,
    FOREIGN KEY (bundle_id) REFERENCES bundle(id)
);

CREATE TABLE activity (
    id          INTEGER PRIMARY KEY,
    package_id  INTEGER NOT NULL,
    title       TEXT NOT NULL,
    weekday     INTEGER NOT NULL,
    start_time  TEXT NOT NULL,
    FOREIGN KEY (package_id) REFERENCES package(id)
);

CREATE TABLE activity_note (
    id          INTEGER PRIMARY KEY,
    activity_id INTEGER NOT NULL,
    note        TEXT NOT NULL,
    FOREIGN KEY (activity_id) REFERENCES activity(id)
);
```

## 2. Consistent aliasing in a view

Views let us consolidate join logic and expose uniform column names. The important rule for
complex mappings is: **alias every column with a stable, prefixed name**. This avoids clashes
and gives the generator a deterministic shape to work with. We also place the dynamic-field
annotations on the view definition so the structure can be reused by any downstream SELECT.

```sql
CREATE VIEW bundle_detailed_view AS
SELECT
    b.id            AS bundle__id,
    b.title         AS bundle__title,
    b.status        AS bundle__status,
    b.provider_id   AS bundle__provider_id,

    p.id            AS provider__id,
    p.name          AS provider__name
/* @@{ dynamicField=provider,
       mappingType=entity,
       sourceTable=p,
       aliasPrefix=provider__ } */
/* @@{ dynamicField=packages,
       mappingType=collection,
       sourceTable=pkg,
       aliasPrefix=package__ } */
/* @@{ dynamicField=notes,
       mappingType=perRow,
       sourceTable=note,
       aliasPrefix=package_activity_note__ } */
FROM bundle b
JOIN provider p ON p.id = b.provider_id;
```

The view contains the base bundle/provider shape along with the annotations for the nested
fields. Downstream queries can join the view with packages, activities, and any other tables
without repeating the annotation blocks—each consumer inherits the same entity/collection
structure.

## 3. Query with nested mappings

Now we build a SELECT that uses the view, joins packages, activities, and notes, and benefits
from the view-level annotations. No `dynamicField` blocks are needed in the query itself:

```sql
-- @@{ queryResult=BundleWithScheduleRow }
SELECT
    v.bundle__id,
    v.bundle__title,
    v.bundle__status,

    v.provider__id     AS provider__id,
    v.provider__name   AS provider__name,

    pkg.id             AS package__id,
    pkg.title          AS package__title,

    act.id             AS package_activity__id,
    act.title          AS package_activity__title,
    act.weekday        AS package_activity__weekday,
    act.start_time     AS package_activity__start_time,

    note.id            AS package_activity_note__id,
    note.note          AS package_activity_note__note
FROM bundle_detailed_view v
LEFT JOIN package pkg
       ON pkg.bundle_id = v.bundle__id
LEFT JOIN activity act
       ON act.package_id = pkg.id
LEFT JOIN activity_note note
       ON note.activity_id = act.id
WHERE v.bundle__status = 'ACTIVE'
ORDER BY v.bundle__title, pkg.title, act.weekday, act.start_time;
```

### Why aliasing matters

- **Unique prefixes** (`provider__`, `package__`, `package_activity__`, etc.) keep the generated
  data classes free of ambiguous names and make it obvious where each field comes from.
- The prefixes line up with the `aliasPrefix` values in the dynamic-field annotations, so
  SQLiteNow can stitch nested structures automatically.
- Even when the source view already uses prefixed columns (e.g., `bundle__status`), we continue
  the convention for joined tables. Consistency pays off when you inevitably extend the query.

### Mapping summary

- `provider` becomes a nested object because of `mappingType=entity` defined on the view.
- `packages` becomes a list of package rows thanks to the view-level `collection` mapping.
- `notes` attaches per-row data directly to the `activity` rows inside each package, and only
  emits when a note exists.

## 4. Additional best practices

1. **Centralize joins in views**: encapsulate the core relationships (bundle ↔ provider) so you can
   reuse them across queries and keep per-query SQL focused on additional data.
2. **Keep alias prefixes short but descriptive**: `package_activity__` is long but removes ambiguity.
   Shorter prefixes (e.g., `act__`) also work—pick a convention and stick with it.
3. **Order your SQL by the same keys you expect in the generated results**: this avoids ordering
   surprises when you turn collections into lists.
4. **Document dynamic fields inline**: we use the comment blocks immediately after the SELECT list so
   future readers understand how each nested structure is built.

With these patterns in place, you can scale to deeper hierarchies by extending the SELECT list,
adding additional joins, and annotating new dynamic fields—without fighting inconsistent column
names or mis-typed parameters.
