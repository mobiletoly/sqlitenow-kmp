UPDATE person
SET first_name = :firstName,
    last_name  = :lastName,
    email      = :email,
    phone      = :phone,
    birth_date = :birthDate,
    ssn        = :ssn,
    score      = :score,
    is_active  = :isActive,
    notes      = :notes,
    updated_at = (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
WHERE id = :id;
