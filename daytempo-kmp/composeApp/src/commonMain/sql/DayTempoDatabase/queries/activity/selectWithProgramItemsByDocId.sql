-- @@{ queryResult=ActivityWithProgramItemsDoc }
SELECT
    act.*

FROM activity_with_program_items_view act
WHERE act.act__doc_id = :docId;
