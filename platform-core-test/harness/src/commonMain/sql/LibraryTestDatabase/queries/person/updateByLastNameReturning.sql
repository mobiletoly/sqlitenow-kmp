UPDATE person
SET first_name = :firstName
WHERE last_name = :lastName
RETURNING id, first_name, last_name, email, phone, birth_date, created_at;
