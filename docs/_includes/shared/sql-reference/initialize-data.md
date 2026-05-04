# Initialize Data

`init/` files seed a database only when it is created for the first time. They
run after schema files and before the database is marked as initialized.

{% if include.platform == "dart" %}
```text
lib/db/sql/AppDatabase/init/
  seed.sql
```
{% elsif include.platform == "kmp" %}
```text
src/commonMain/sql/AppDatabase/init/
  seed.sql
```
{% endif %}

```sql
INSERT INTO task (id, title, completed, created_at)
VALUES (1, 'Review schedule', 0, '2026-01-01T09:00:00Z');

INSERT INTO task (id, title, completed, created_at)
VALUES (2, 'Prepare visit notes', 0, '2026-01-01T10:00:00Z');
```

Use `init/` for fresh-install seed data that should not run during upgrades.
Use `migration/` for changes that installed databases must receive.

Multiple `.sql` files can be used; they are executed in deterministic file-name
order.
