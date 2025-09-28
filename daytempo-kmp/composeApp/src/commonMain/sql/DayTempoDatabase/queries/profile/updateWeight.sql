UPDATE profile
SET body_weight = :bodyWeight, body_weight_last_update = :bodyWeightLastUpdate
WHERE name = 'main' AND body_weight_last_update <= :bodyWeightLastUpdate;
