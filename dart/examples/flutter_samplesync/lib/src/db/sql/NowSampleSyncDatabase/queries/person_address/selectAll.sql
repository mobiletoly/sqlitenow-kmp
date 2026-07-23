-- @@{ queryResult=PersonAddressRow }
SELECT *
FROM person_address
WHERE person_id = :personId
ORDER BY created_at DESC;
