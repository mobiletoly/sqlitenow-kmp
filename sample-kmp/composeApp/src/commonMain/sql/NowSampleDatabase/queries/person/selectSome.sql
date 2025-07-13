SELECT
    p.id,
    p.first_name AS hello_first_name,
    p.last_name,
    p.email
FROM Person p
ORDER BY id DESC
LIMIT :limit OFFSET :offset
