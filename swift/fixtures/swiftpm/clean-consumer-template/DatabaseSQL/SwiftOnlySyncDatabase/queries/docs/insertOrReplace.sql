INSERT INTO docs (doc_id, title)
VALUES (:docId, :title)
ON CONFLICT(doc_id) DO UPDATE SET title = excluded.title;
