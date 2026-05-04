-- @@{ queryResult=PersonRow }
SELECT id, name, status, score, avatar
FROM person
WHERE id = :id;
