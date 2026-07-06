-- @@{ queryResult=DocRow }
SELECT id, status
FROM doc
WHERE status IN :statuses
ORDER BY id;
