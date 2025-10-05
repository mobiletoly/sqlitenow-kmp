UPDATE program_item
SET
    item_id = :itemId,
    title = :title,
    descr = :descr,
    goal_value = :goalValue,
    goal_daily_initial = :goalDailyInitial,
    goal_direction = :goalDirection,
    goal_invert = :goalInvert,
    goal_at_least = :goalAtLeast,
    goal_single = :goalSingle,
    pre_start_text = :preStartText,
    post_complete_text = :postCompleteText,
    input_entries = :inputEntries
WHERE doc_id = :docId;
