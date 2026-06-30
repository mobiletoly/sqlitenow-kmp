CREATE TABLE task (
    id INTEGER PRIMARY KEY NOT NULL,
    title TEXT NOT NULL,
    -- @@{ field=is_done, propertyType=Boolean }
    is_done INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    payload BLOB,
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_task_created_at ON task (created_at, id);
