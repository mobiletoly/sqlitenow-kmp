-- @@{ sharedResult=Row }
SELECT
    id,
    person_id,
    comment,
    created_at,
    tags
FROM comment
WHERE person_id = :personId
