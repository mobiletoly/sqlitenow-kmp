-- @@{ queryResult=PersonRow }
SELECT * FROM person
WHERE last_name IN :lastNames;
