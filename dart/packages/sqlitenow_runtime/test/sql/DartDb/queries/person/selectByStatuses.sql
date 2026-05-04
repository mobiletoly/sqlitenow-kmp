-- @@{ queryResult=PersonRow }
SELECT p.id, p.name, p.status, p.score, p.avatar
FROM person p
WHERE p.status IN :statuses
ORDER BY p.id;
