DELETE FROM person
WHERE id = :id
RETURNING id, first_name, last_name, email, phone, birth_date, created_at;
