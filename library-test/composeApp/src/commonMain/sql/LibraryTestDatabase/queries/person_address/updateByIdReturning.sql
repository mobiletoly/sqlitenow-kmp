UPDATE person_address
SET person_id = :personId,
    address_type = :addressType,
    street = :street,
    city = :city,
    state = :state,
    postal_code = :postalCode,
    country = :country,
    is_primary = :isPrimary
WHERE id = :id
RETURNING id, person_id, address_type, street, city, state, postal_code, country, is_primary;
