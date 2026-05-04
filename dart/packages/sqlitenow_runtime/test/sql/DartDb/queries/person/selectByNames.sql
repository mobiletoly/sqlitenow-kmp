-- @@{ queryResult=PersonRow }
SELECT p.id, p.name, p.status, p.score, p.avatar
FROM person p
WHERE p.name IN :names
ORDER BY p.id;
