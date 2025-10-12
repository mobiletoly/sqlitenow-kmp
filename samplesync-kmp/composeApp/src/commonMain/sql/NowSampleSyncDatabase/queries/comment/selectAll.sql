-- @@{ queryResult=CommentRow }
SELECT
    id,
    person_id,
    comment,
    created_at,
    tags
FROM comment
WHERE person_id = :personId
ORDER BY created_at DESC
