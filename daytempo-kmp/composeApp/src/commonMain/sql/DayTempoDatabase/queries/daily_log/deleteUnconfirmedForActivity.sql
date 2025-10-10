DELETE FROM daily_log
WHERE activity_doc_id = :activityDocId
AND confirmed = 0;
