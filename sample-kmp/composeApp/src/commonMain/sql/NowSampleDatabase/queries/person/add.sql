INSERT INTO person(email,
                   first_name,
                   last_name,
                   phone,
                   birth_date,
                   notes)
VALUES (:email,
        :firstName,
        :lastName,
        :phone,
        :birthDate,
        :notes)
ON CONFLICT(email) DO UPDATE SET first_name = :firstName,
                                 last_name  = :lastName,
                                 phone      = :phone,
                                 birth_date = :birthDate,
                                 notes      = :notes
RETURNING *;
