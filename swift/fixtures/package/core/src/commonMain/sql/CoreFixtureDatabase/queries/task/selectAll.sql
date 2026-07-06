-- @@{ queryResult=TaskRow }
SELECT id, title, is_done, created_at, payload, priority
FROM task
ORDER BY created_at, id;
