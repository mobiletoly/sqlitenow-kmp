CREATE TABLE program_item (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    activity_doc_id TEXT NOT NULL REFERENCES activity(doc_id) ON DELETE CASCADE,

    item_id TEXT NOT NULL,

    title TEXT,

    descr TEXT,

    -- @@{ field=goal_value, propertyType=kotlin.Int }
    goal_value INTEGER NOT NULL,

    -- @@{ field=goal_daily_initial, propertyType=kotlin.Int }
    goal_daily_initial INTEGER NOT NULL,

    -- @@{ field=goal_direction, propertyType=com.pluralfusion.daytempo.domain.model.GoalDirection }
    goal_direction TEXT NOT NULL,

    -- @@{ field=goal_invert, propertyType=kotlin.Boolean }
    goal_invert INTEGER NOT NULL,

    -- @@{ field=goal_at_least, propertyType=kotlin.Boolean }
    goal_at_least INTEGER NOT NULL,

    -- @@{ field=goal_single, propertyType=kotlin.Boolean }
    goal_single INTEGER NOT NULL,

    -- @@{ field=goal_hide_editor, propertyType=kotlin.Boolean }
    goal_hide_editor INTEGER NOT NULL,

    -- @@{ field=week_index, propertyType=kotlin.Int }
    week_index INTEGER NOT NULL,

    -- @@{ field=day_index, propertyType=kotlin.Int }
    day_index INTEGER NOT NULL,

    pre_start_text TEXT,

    post_complete_text TEXT,

    -- @@{ field=presentation, propertyType=com.pluralfusion.daytempo.domain.model.ProgramItemPresentation }
    presentation TEXT,

    seq_items_json TEXT NOT NULL,

    required_unlock_code TEXT,

    -- @@{ field=has_unlocked_seq_items, propertyType=kotlin.Boolean }
    has_unlocked_seq_items INTEGER NOT NULL,

    -- @@{ field=lock_item_display, propertyType=com.pluralfusion.daytempo.domain.model.ProgramItemLockItemDisplay }
    lock_item_display TEXT NOT NULL,

    -- @@{ field=input_entries, propertyType=kotlin.collections.List<com.pluralfusion.daytempo.domain.model.ProgramItemInputEntry> }
    input_entries TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX idx_programItem_activityDocId ON program_item(activity_doc_id);
CREATE INDEX idx_programItem_weekIndex ON program_item(week_index);
CREATE INDEX idx_programItem_dayIndex ON program_item(day_index);

CREATE VIEW program_item_to_join AS
SELECT
    id AS pi__id,
    doc_id AS pi__doc_id,
    activity_doc_id AS pi__activity_doc_id,
    item_id AS pi__item_id,
    title AS pi__title,
    descr AS pi__descr,
    goal_value AS pi__goal_value,
    goal_daily_initial AS pi__goal_daily_initial,
    goal_direction AS pi__goal_direction,
    goal_invert AS pi__goal_invert,
    goal_at_least AS pi__goal_at_least,
    goal_single AS pi__goal_single,
    goal_hide_editor AS pi__goal_hide_editor,
    week_index AS pi__week_index,
    day_index AS pi__day_index,
    pre_start_text AS pi__pre_start_text,
    post_complete_text AS pi__post_complete_text,
    presentation AS pi__presentation,
    seq_items_json AS pi__seq_items_json,
    required_unlock_code AS pi__required_unlock_code,
    has_unlocked_seq_items AS pi__has_unlocked_seq_items,
    lock_item_display AS pi__lock_item_display,
    input_entries AS pi__input_entries
FROM program_item pi;
