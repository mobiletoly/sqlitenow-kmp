UPDATE daily_log
SET counter = :counter
WHERE doc_id = :docId;
