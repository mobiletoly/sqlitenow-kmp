-- @@{ queryResult=ActivityWithProgramItemsRow }
SELECT * FROM activity_with_program_items_view
WHERE act__enabled = 1
    AND act__deleted = 0;
