UPDATE person_address
SET state       = :state,
    is_primary  = 0,
    postal_code = NULL,
    country     = 'CA',
    created_at  = CURRENT_TIMESTAMP
WHERE id = :addressId
RETURNING *;
