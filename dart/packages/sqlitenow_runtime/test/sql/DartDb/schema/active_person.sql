CREATE VIEW active_person AS
SELECT id, name, status, score, avatar
FROM person
WHERE status = 'active';
