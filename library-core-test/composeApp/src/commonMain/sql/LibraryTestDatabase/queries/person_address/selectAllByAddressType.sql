-- @@{ queryResult=PersonAddressRow }
SELECT *
FROM person_address
WHERE address_type = :address_type
