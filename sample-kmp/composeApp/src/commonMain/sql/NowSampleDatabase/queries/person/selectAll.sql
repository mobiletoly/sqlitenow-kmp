-- @@{ sharedResult=Row }
SELECT
    *
FROM Person
ORDER BY id DESC
LIMIT :limit OFFSET :offset
