INSERT INTO person_address(person_id,
                           address_type,
                           street,
                           city,
                           state,
                           postal_code,
                           country,
                           is_primary,
                           created_at)
VALUES (:personId,
        :addressType,
        :street,
        :city,
        NULL,
        NULL,
        'US',
        1,
        strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
RETURNING *;
