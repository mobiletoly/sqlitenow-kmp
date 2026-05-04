INSERT INTO person(id, name, status, score, avatar)
VALUES (:id, :name, :status, :score, :avatar)
RETURNING id, name, status, score, avatar;
