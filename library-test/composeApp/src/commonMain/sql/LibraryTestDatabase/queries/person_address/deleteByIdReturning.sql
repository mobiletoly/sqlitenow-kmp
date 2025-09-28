DELETE FROM person_address
WHERE id = :id
RETURNING id, person_id, address_type, street, city, state, postal_code, country, is_primary;
