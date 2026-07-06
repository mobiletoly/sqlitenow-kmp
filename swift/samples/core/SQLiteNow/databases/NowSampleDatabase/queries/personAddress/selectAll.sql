-- @@{ queryResult=PersonAddressRow }
SELECT *
FROM person_address
ORDER BY person_id, is_primary DESC, id;
