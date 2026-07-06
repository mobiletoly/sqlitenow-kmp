-- @@{ queryResult=TaskRow }
SELECT id, title, is_done, created_at, payload, priority
FROM task
WHERE id = :id;
