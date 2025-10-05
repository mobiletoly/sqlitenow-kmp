-- @@{ queryResult=ProgramItemRow }
SELECT id, doc_id, activity_doc_id, item_id, title, descr,
    goal_value, goal_daily_initial, goal_direction, goal_invert, goal_at_least, goal_single,
    goal_hide_editor, week_index, day_index, pre_start_text, post_complete_text,
    presentation, seq_items_json, required_unlock_code, has_unlocked_seq_items,
    lock_item_display, input_entries
FROM program_item;
