-- @@{ queryResult=PersonRow }
SELECT
    *
FROM person
ORDER BY id DESC
LIMIT :limit OFFSET :offset
