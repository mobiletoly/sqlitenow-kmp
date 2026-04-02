UPDATE person
SET first_name = :firstName,
    last_name  = :lastName,
    email      = :email,
    phone      = :phone,
    birth_date = :birthDate
WHERE id = :id;
