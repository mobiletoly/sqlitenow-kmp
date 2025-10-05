-- @@{ queryResult=ProfileRow }
SELECT id, name, measure_system, temperature_system, body_weight, body_weight_last_update,
    body_height, body_height_last_update, unlock_codes, essential_plan, gender, birthday
FROM profile WHERE name = 'main';
