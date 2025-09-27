INSERT INTO person(email,
                   first_name,
                   last_name,
                   phone,
                   birth_date)
VALUES (:email,
        :firstName,
        :lastName,
        :phone,
        :birthDate)
ON CONFLICT(email) DO UPDATE SET first_name = :firstName,
                                 last_name  = :lastName,
                                 phone      = :phone,
                                 birth_date = :birthDate
RETURNING *;
