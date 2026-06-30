ALTER TABLE task ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
UPDATE task
SET title = '\(migration literal)'
WHERE 0;
