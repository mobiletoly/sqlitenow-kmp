-- @@{ queryResult=ActivityDetailedDoc }
SELECT
    act.*

FROM activity_detailed_view act
WHERE act.act__doc_id = :docId;
