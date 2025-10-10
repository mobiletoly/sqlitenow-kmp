-- @@{ queryResult=DailyLogRow }
SELECT
    dl.*
FROM daily_log dl
WHERE dl.date BETWEEN :start AND :end
AND dl.activity_doc_id = :activityDocId;
