UPDATE daily_log
SET counter = :counter,
    program_item_doc_id = :programItemDocId,
    applied_week_ind = :weekIndex,
    applied_day_ind = :dayIndex
WHERE doc_id = :docId;
