UPDATE person_address
SET state       = :state,
    is_primary  = 0,
    postal_code = NULL,
    country     = 'CA',
    created_at  = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE id = :addressId
RETURNING *;
