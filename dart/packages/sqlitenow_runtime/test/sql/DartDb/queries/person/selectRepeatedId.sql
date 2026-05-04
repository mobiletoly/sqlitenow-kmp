-- @@{ queryResult=PersonRow }
SELECT p.id, p.name, p.status, p.score, p.avatar
FROM person p
WHERE p.id = :id OR p.id = :id
ORDER BY p.id;
