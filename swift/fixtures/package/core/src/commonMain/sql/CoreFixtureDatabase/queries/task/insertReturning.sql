-- @@{ queryResult=TaskRow }
INSERT INTO task (id, title, is_done, created_at, payload, priority)
VALUES (:id, :title, :isDone, :createdAt, :payload, :priority)
RETURNING id, title, is_done, created_at, payload, priority;
