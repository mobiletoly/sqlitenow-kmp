INSERT INTO person_category (person_id, category_id, is_primary) VALUES (:personId, :categoryId, :isPrimary) RETURNING *
