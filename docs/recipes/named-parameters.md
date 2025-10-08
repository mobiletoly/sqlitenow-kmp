---
layout: doc
title: Named Parameters
permalink: /recipes/named-parameters/
parent: Recipes
---

# Named Parameters

SQLiteNow infers parameter types by analysing how each named argument is used. When a parameter
binds directly to a column, the generator picks up the type automatically. Complex expressions,
``WITH`` clauses, or functions can hide that context and default the parameter to `String`.

Use explicit casts to steer the generator toward the correct Kotlin type.

## Enforcing numeric types in date calculations

Consider a query that derives a date range from `:year` and `:month` arguments:

```sql
WITH range AS (
  SELECT
    (julianday(printf('%04d-%02d-01', :year, :month)) - julianday('1970-01-01')) AS start_days,
    (julianday(date(printf('%04d-%02d-01', :year, :month), '+1 month')) - julianday('1970-01-01')) AS end_days
)
SELECT *
FROM daily_log
WHERE log_date >= (SELECT start_days FROM range)
  AND log_date <  (SELECT end_days FROM range);
```

Because `:year` and `:month` never bind to a column directly, the generator cannot infer their
types and defaults to `String`.

Add explicit casts to guide type inference:

```sql
WITH range AS (
  SELECT
    (julianday(printf('%04d-%02d-01', CAST(:year AS INTEGER), CAST(:month AS INTEGER))) - julianday('1970-01-01')) AS start_days,
    (julianday(date(printf('%04d-%02d-01', :year, :month), '+1 month')) - julianday('1970-01-01')) AS end_days
)
SELECT *
FROM daily_log
WHERE log_date >= (SELECT start_days FROM range)
  AND log_date <  (SELECT end_days FROM range);
```

With the casts in place, SQLiteNow detects that both parameters should be numeric (`Int`/`Long`
depending on your schema), and the generated `Params` data class uses the appropriate Kotlin type.

## When to cast parameters

- Parameters used only inside SQL functions (`julianday`, `strftime`, etc.)
- Expressions inside `WITH` CTEs that never bind to a physical column
- JSON or math functions that strip the original column context

When in doubt, casting to the expected column type prevents accidental `String` fallbacks and keeps
your API strongly typed.
