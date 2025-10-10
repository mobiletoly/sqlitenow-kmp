-- @@{ queryResult=DailyLogRow }
SELECT
    dl.*
FROM daily_log dl
WHERE dl.group_doc_id = :groupDocId
AND dl.date BETWEEN :start AND :end;
