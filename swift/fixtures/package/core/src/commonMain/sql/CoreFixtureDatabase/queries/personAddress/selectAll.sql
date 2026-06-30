-- @@{ queryResult=PersonAddressRow }
SELECT id, person_id, street, is_primary
FROM person_address
ORDER BY id;
