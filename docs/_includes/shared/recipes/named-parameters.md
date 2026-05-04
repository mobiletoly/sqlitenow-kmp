# Named Parameters

SQLiteNow infers parameter types from how each named argument is used. When a
parameter binds directly to a column, the generator can usually infer the type
from the schema.

```sql
SELECT id, title
FROM task
WHERE id = :id;
```

Complex expressions can hide that context. Use explicit casts when a parameter
is only used inside a function, CTE, JSON expression, or arithmetic expression:

```sql
WITH range AS (
  SELECT
    date(printf('%04d-%02d-01', CAST(:year AS INTEGER), CAST(:month AS INTEGER))) AS start_date
)
SELECT *
FROM task
WHERE created_at >= (SELECT start_date FROM range);
```

{% if include.platform == "dart" %}
The generated Dart params class then exposes numeric fields instead of falling
back to strings:

```dart
await db.task.selectForMonth(TaskSelectForMonthParams(year: 2026, month: 5));
```
{% elsif include.platform == "kmp" %}
The generated KMP params class then exposes numeric fields instead of falling
back to strings:

```kotlin
db.task.selectForMonth(TaskQuery.SelectForMonth.Params(year = 2026, month = 5))
```
{% endif %}
