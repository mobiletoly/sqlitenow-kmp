UPDATE person_address
SET is_primary = :isPrimary,
    city = :newCity
WHERE city = :oldCity
RETURNING id, person_id, address_type, street, city, state, postal_code, country, is_primary, created_at;
