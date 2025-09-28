UPDATE profile
SET body_height = :bodyHeight, body_height_last_update = :bodyHeightLastUpdate
WHERE name = 'main' AND body_height_last_update <= :bodyHeightLastUpdate;
