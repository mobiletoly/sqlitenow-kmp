INSERT INTO person_address (person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (:personId, :addressType, :street, :city, :state, :postalCode, :country, :isPrimary)
RETURNING id, person_id, address_type, street, city, state, postal_code, country, is_primary, created_at;
