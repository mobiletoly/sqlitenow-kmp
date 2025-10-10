DELETE FROM daily_log
WHERE EXISTS (
  SELECT 1 FROM activity
  WHERE activity.doc_id = daily_log.activity_doc_id
    AND activity.delete_when_expired = 1
)
AND date < :olderThan;
