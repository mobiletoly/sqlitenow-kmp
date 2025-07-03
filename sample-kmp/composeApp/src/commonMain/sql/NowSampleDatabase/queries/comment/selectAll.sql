SELECT
    id,
    person_id,
    comment,
    created_at,
    tags
FROM Comment
WHERE person_id = :personId
