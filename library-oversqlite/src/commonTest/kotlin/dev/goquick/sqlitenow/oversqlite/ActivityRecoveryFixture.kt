package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.Json

// Maintained synthetic copy of the pinned activity schema inventory, never runtime checkout input.
// Reference HEAD: 3aede22195a7462ee0178bf64f4a054d5440dc01
// SQLite fixture SHA256: 8cfed735519b5023ee9b73c8db409006d2809185c9a2d1da1fcb3c110c84b1b3
internal object ActivityRecoveryFixture {
    fun generation(schema: String = "main"): SyncGeneration {
        val parsed = Json.decodeFromString(SyncGeneration.serializer(), generationJson)
        val mapped = parsed.copy(tables = parsed.tables.map { table -> table.copy(schema = schema,
            foreignKeys = table.foreignKeys.map { it.copy(referencedSchema = schema) }) })
        return mapped.copy(fingerprint = mapped.computeFingerprint())
    }
    val tables: List<SyncTable> get() = generation().tables.map { SyncTable(it.table,syncKeyColumns=it.syncKeyColumns) }
    suspend fun create(db: SafeSQLiteConnection) { for (sql in schemaStatements) db.execSQL(sql) }
    suspend fun seed(db: SafeSQLiteConnection) { for (sql in seedStatements) db.execSQL(sql) }
    private const val generationJson = """{"id":"1","fingerprint":"","tables":[{"schema":"main","table":"activity","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"content_origin","wire_type":"text","nullable":false},{"name":"group_id","wire_type":"text","nullable":false},{"name":"activity_package_id","wire_type":"text","nullable":false},{"name":"category_id","wire_type":"text","nullable":false},{"name":"program_type","wire_type":"text","nullable":false},{"name":"delete_when_expired","wire_type":"boolean","nullable":false},{"name":"days_confirm_required","wire_type":"boolean","nullable":false},{"name":"order_ind","wire_type":"int64","nullable":false},{"name":"title","wire_type":"text","nullable":false},{"name":"descr","wire_type":"text","nullable":false},{"name":"icon","wire_type":"text","nullable":false},{"name":"required_unlock_code","wire_type":"text","nullable":true},{"name":"priority","wire_type":"int64","nullable":false},{"name":"unlocked_days","wire_type":"int64","nullable":true},{"name":"reporting","wire_type":"text","nullable":false},{"name":"semantics_json","wire_type":"text","nullable":false},{"name":"meta_json","wire_type":"text","nullable":true},{"name":"sched_mandatory_to_setup","wire_type":"boolean","nullable":false},{"name":"sched_allowed_repeat_modes","wire_type":"text","nullable":false},{"name":"sched_repeat","wire_type":"text","nullable":false},{"name":"sched_days_of_week_mask","wire_type":"int64","nullable":false},{"name":"sched_weeks_mask","wire_type":"int64","nullable":false},{"name":"sched_days_of_month_mask","wire_type":"int64","nullable":false},{"name":"sched_start_at","wire_type":"text","nullable":true},{"name":"sched_start_at_eval","wire_type":"text","nullable":true},{"name":"sched_read_ref_start_at","wire_type":"text","nullable":true},{"name":"sched_write_ref_start_at","wire_type":"text","nullable":true},{"name":"sched_start_at_label","wire_type":"text","nullable":true},{"name":"sched_repeat_after_days","wire_type":"int64","nullable":false},{"name":"sched_read_ref_repeat_after_days","wire_type":"text","nullable":true},{"name":"sched_write_ref_repeat_after_days","wire_type":"text","nullable":true},{"name":"sched_repeat_after_days_label","wire_type":"text","nullable":true},{"name":"sched_repeat_after_days_min","wire_type":"int64","nullable":true},{"name":"sched_repeat_after_days_max","wire_type":"int64","nullable":true},{"name":"sched_days_duration","wire_type":"int64","nullable":false},{"name":"sched_allow_edit_days_duration","wire_type":"boolean","nullable":false},{"name":"sched_read_ref_days_duration","wire_type":"text","nullable":true},{"name":"sched_write_ref_days_duration","wire_type":"text","nullable":true},{"name":"sched_days_duration_label","wire_type":"text","nullable":true},{"name":"sched_days_duration_min","wire_type":"int64","nullable":true},{"name":"sched_days_duration_max","wire_type":"int64","nullable":true},{"name":"sched_time_points_json","wire_type":"text","nullable":false},{"name":"sched_time_range","wire_type":"text","nullable":true}],"foreign_keys":[{"columns":["category_id"],"referenced_schema":"main","referenced_table":"activity_category","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"},{"columns":["activity_package_id"],"referenced_schema":"main","referenced_table":"activity_package","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"activity_bundle","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"publisher_id","wire_type":"text","nullable":true},{"name":"content_origin","wire_type":"text","nullable":false},{"name":"publisher_title","wire_type":"text","nullable":false},{"name":"version","wire_type":"int64","nullable":false},{"name":"title","wire_type":"text","nullable":false},{"name":"descr","wire_type":"text","nullable":true},{"name":"purchase_mode","wire_type":"text","nullable":false},{"name":"unlock_code","wire_type":"text","nullable":true},{"name":"resources_json","wire_type":"text","nullable":false},{"name":"icon","wire_type":"text","nullable":false},{"name":"promo_image","wire_type":"text","nullable":false},{"name":"promo_screens_json","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["publisher_id"],"referenced_schema":"main","referenced_table":"publisher","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"activity_category","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"title","wire_type":"text","nullable":false},{"name":"icon","wire_type":"text","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"activity_package","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"content_origin","wire_type":"text","nullable":false},{"name":"activity_bundle_id","wire_type":"text","nullable":false},{"name":"title","wire_type":"text","nullable":false},{"name":"descr","wire_type":"text","nullable":true},{"name":"pre_start_text","wire_type":"text","nullable":true},{"name":"category_id","wire_type":"text","nullable":false},{"name":"icon","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["category_id"],"referenced_schema":"main","referenced_table":"activity_category","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"},{"columns":["activity_bundle_id"],"referenced_schema":"main","referenced_table":"activity_bundle","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"activity_state","sync_key_columns":["activity_id"],"columns":[{"name":"activity_id","wire_type":"text","nullable":false},{"name":"enabled","wire_type":"boolean","nullable":false},{"name":"monthly_glance_view","wire_type":"boolean","nullable":false},{"name":"priority","wire_type":"int64","nullable":false},{"name":"category_id","wire_type":"text","nullable":false},{"name":"program_type","wire_type":"text","nullable":false},{"name":"installed_at","wire_type":"text","nullable":true},{"name":"repeat","wire_type":"text","nullable":false},{"name":"days_of_week_mask","wire_type":"int64","nullable":false},{"name":"weeks_mask","wire_type":"int64","nullable":false},{"name":"days_of_month_mask","wire_type":"int64","nullable":false},{"name":"start_at","wire_type":"text","nullable":true},{"name":"repeat_after_days","wire_type":"int64","nullable":false},{"name":"days_duration","wire_type":"int64","nullable":false},{"name":"time_points_json","wire_type":"text","nullable":false},{"name":"time_range","wire_type":"text","nullable":true}],"foreign_keys":[{"columns":["category_id"],"referenced_schema":"main","referenced_table":"activity_category","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"},{"columns":["activity_id"],"referenced_schema":"main","referenced_table":"activity","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"bundle_state","sync_key_columns":["bundle_id"],"columns":[{"name":"bundle_id","wire_type":"text","nullable":false},{"name":"purchased","wire_type":"boolean","nullable":false},{"name":"installed_at","wire_type":"text","nullable":true}],"foreign_keys":[{"columns":["bundle_id"],"referenced_schema":"main","referenced_table":"activity_bundle","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"daily_log","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"parent_daily_log_id","wire_type":"text","nullable":true},{"name":"activity_id","wire_type":"text","nullable":false},{"name":"program_item_id","wire_type":"text","nullable":false},{"name":"group_id","wire_type":"text","nullable":true},{"name":"date","wire_type":"text","nullable":false},{"name":"applied_week_ind","wire_type":"int64","nullable":false},{"name":"applied_day_ind","wire_type":"int64","nullable":false},{"name":"counter","wire_type":"int64","nullable":false},{"name":"added_manually","wire_type":"boolean","nullable":false},{"name":"confirmed","wire_type":"boolean","nullable":false},{"name":"inputs_json","wire_type":"text","nullable":false},{"name":"notes","wire_type":"text","nullable":true}],"foreign_keys":[{"columns":["program_item_id"],"referenced_schema":"main","referenced_table":"program_item","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"},{"columns":["activity_id"],"referenced_schema":"main","referenced_table":"activity","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"},{"columns":["parent_daily_log_id"],"referenced_schema":"main","referenced_table":"daily_log","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"profile","sync_key_columns":["name"],"columns":[{"name":"name","wire_type":"text","nullable":false},{"name":"measure_system","wire_type":"text","nullable":false},{"name":"temperature_system","wire_type":"text","nullable":false},{"name":"body_weight","wire_type":"int64","nullable":false},{"name":"body_weight_last_update","wire_type":"text","nullable":false},{"name":"body_height","wire_type":"int64","nullable":false},{"name":"body_height_last_update","wire_type":"text","nullable":false},{"name":"unlock_codes","wire_type":"text","nullable":false},{"name":"essential_plan","wire_type":"boolean","nullable":false},{"name":"gender","wire_type":"text","nullable":false},{"name":"birthday","wire_type":"text","nullable":false},{"name":"time_zone","wire_type":"text","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"program_item","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"activity_id","wire_type":"text","nullable":false},{"name":"item_id","wire_type":"text","nullable":false},{"name":"title","wire_type":"text","nullable":true},{"name":"descr","wire_type":"text","nullable":true},{"name":"goal_value","wire_type":"int64","nullable":false},{"name":"goal_daily_initial","wire_type":"int64","nullable":false},{"name":"goal_direction","wire_type":"text","nullable":false},{"name":"goal_invert","wire_type":"boolean","nullable":false},{"name":"goal_at_least","wire_type":"boolean","nullable":false},{"name":"goal_single","wire_type":"boolean","nullable":false},{"name":"goal_hide_editor","wire_type":"boolean","nullable":false},{"name":"week_index","wire_type":"int64","nullable":false},{"name":"day_index","wire_type":"int64","nullable":false},{"name":"pre_start_text","wire_type":"text","nullable":true},{"name":"post_complete_text","wire_type":"text","nullable":true},{"name":"presentation","wire_type":"text","nullable":true},{"name":"seq_items_json","wire_type":"text","nullable":false},{"name":"required_unlock_code","wire_type":"text","nullable":true},{"name":"has_unlocked_seq_items","wire_type":"boolean","nullable":false},{"name":"lock_item_display","wire_type":"text","nullable":false},{"name":"input_entries","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["activity_id"],"referenced_schema":"main","referenced_table":"activity","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"program_item_input_goal","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"program_item_id","wire_type":"text","nullable":false},{"name":"input_key","wire_type":"text","nullable":false},{"name":"current_revision","wire_type":"int64","nullable":false}],"foreign_keys":[{"columns":["program_item_id"],"referenced_schema":"main","referenced_table":"program_item","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"program_item_input_goal_revision","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"goal_id","wire_type":"text","nullable":false},{"name":"revision","wire_type":"int64","nullable":false},{"name":"effective_day","wire_type":"text","nullable":false},{"name":"goal_kind","wire_type":"text","nullable":false},{"name":"policy_json","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["goal_id"],"referenced_schema":"main","referenced_table":"program_item_input_goal","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"program_item_state","sync_key_columns":["program_item_id"],"columns":[{"name":"program_item_id","wire_type":"text","nullable":false},{"name":"goal_value","wire_type":"int64","nullable":false},{"name":"goal_invert","wire_type":"boolean","nullable":false},{"name":"goal_single","wire_type":"boolean","nullable":false}],"foreign_keys":[{"columns":["program_item_id"],"referenced_schema":"main","referenced_table":"program_item","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"publisher","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"title","wire_type":"text","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"value_registry","sync_key_columns":["key"],"columns":[{"name":"key","wire_type":"text","nullable":false},{"name":"value_type","wire_type":"text","nullable":false},{"name":"updated_at","wire_type":"text","nullable":false},{"name":"string_value","wire_type":"text","nullable":true},{"name":"numeric_value","wire_type":"float64","nullable":true}],"foreign_keys":[]}]}"""
    private val schemaStatements = listOf(
"""-- Synthetic qualification fixture for the activity schema; reference revision 3aede22195a7462ee0178bf64f4a054d5440dc01.
-- Schema declarations preserve source SQLite shapes; app_settings remains local-only.
-- Explicit wire mapping: annotated INTEGER booleans -> boolean; other INTEGER -> int64, REAL -> float64, all TEXT -> text (including date/JSON text).
-- Source activity.sql SHA256 9e353dff33d4f044934b9aa5869a1627ed79bc93747ac95e99b4caf230b2cc60
CREATE TABLE activity (
    id TEXT PRIMARY KEY NOT NULL,

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    group_id TEXT NOT NULL,

    activity_package_id TEXT NOT NULL REFERENCES activity_package(id),

    category_id TEXT NOT NULL REFERENCES activity_category(id),

    program_type TEXT NOT NULL CHECK (program_type IN ('marker', 'simple', 'finite', 'dailyCounter')),

    delete_when_expired INTEGER NOT NULL DEFAULT 0 CHECK (delete_when_expired IN (0,1)),

    days_confirm_required INTEGER NOT NULL DEFAULT 0 CHECK (days_confirm_required IN (0,1)),

    order_ind INTEGER NOT NULL CHECK (order_ind >= 0),

    title TEXT NOT NULL,

    descr TEXT NOT NULL,

    icon TEXT NOT NULL,

    required_unlock_code TEXT,

    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority >= 0),

    unlocked_days INTEGER,

    reporting TEXT NOT NULL DEFAULT 'default' CHECK (reporting IN ('default', 'disabled')),

    semantics_json TEXT NOT NULL DEFAULT '{"tracks":[],"emits":[]}',

    meta_json TEXT,

    sched_mandatory_to_setup INTEGER NOT NULL DEFAULT 0 CHECK (sched_mandatory_to_setup IN (0,1)),

    sched_allowed_repeat_modes TEXT NOT NULL DEFAULT '[]',

    sched_repeat TEXT NOT NULL CHECK (sched_repeat IN ('none', 'weekDays', 'daysOfMonth', 'daysInterval')),

    sched_days_of_week_mask INTEGER NOT NULL DEFAULT 0 CHECK (sched_days_of_week_mask BETWEEN 0 AND 127),

    sched_weeks_mask INTEGER NOT NULL DEFAULT 0 CHECK (sched_weeks_mask BETWEEN 0 AND 15),

    sched_days_of_month_mask INTEGER NOT NULL DEFAULT 0 CHECK (sched_days_of_month_mask BETWEEN 0 AND 2147483647),

    sched_start_at TEXT,

    sched_start_at_eval TEXT,

    sched_read_ref_start_at TEXT,

    sched_write_ref_start_at TEXT,

    sched_start_at_label TEXT,

    sched_repeat_after_days INTEGER NOT NULL
        CHECK (sched_repeat <> 'daysInterval' OR sched_repeat_after_days > 0),

    sched_read_ref_repeat_after_days TEXT,

    sched_write_ref_repeat_after_days TEXT,

    sched_repeat_after_days_label TEXT,

    sched_repeat_after_days_min INTEGER,

    sched_repeat_after_days_max INTEGER,

    sched_days_duration INTEGER NOT NULL
        CHECK (sched_repeat <> 'daysInterval' OR sched_days_duration > 0),

    sched_allow_edit_days_duration INTEGER NOT NULL DEFAULT 0 CHECK (sched_allow_edit_days_duration IN (0,1)),

    sched_read_ref_days_duration TEXT,

    sched_write_ref_days_duration TEXT,

    sched_days_duration_label TEXT,

    sched_days_duration_min INTEGER,

    sched_days_duration_max INTEGER,

    sched_time_points_json TEXT NOT NULL DEFAULT '[]',

    sched_time_range TEXT CHECK (sched_time_range IS NULL OR sched_time_range IN ('morning', 'afternoon', 'evening'))
) WITHOUT ROWID;""",
"""CREATE TABLE activity_state (
    activity_id TEXT PRIMARY KEY NOT NULL REFERENCES activity(id) ON DELETE CASCADE,

    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),

    monthly_glance_view INTEGER NOT NULL CHECK (monthly_glance_view IN (0,1)),

    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority >= 0),

    category_id TEXT NOT NULL REFERENCES activity_category(id),

    program_type TEXT NOT NULL CHECK (program_type IN ('marker', 'simple', 'finite', 'dailyCounter')),

    installed_at TEXT,

    repeat TEXT NOT NULL CHECK (repeat IN ('none', 'weekDays', 'daysOfMonth', 'daysInterval')),

    days_of_week_mask INTEGER NOT NULL DEFAULT 0 CHECK (days_of_week_mask BETWEEN 0 AND 127),

    weeks_mask INTEGER NOT NULL DEFAULT 0 CHECK (weeks_mask BETWEEN 0 AND 15),

    days_of_month_mask INTEGER NOT NULL DEFAULT 0 CHECK (days_of_month_mask BETWEEN 0 AND 2147483647),

    start_at TEXT,

    repeat_after_days INTEGER NOT NULL
        CHECK (repeat <> 'daysInterval' OR repeat_after_days > 0),

    days_duration INTEGER NOT NULL
        CHECK (repeat <> 'daysInterval' OR days_duration > 0),

    time_points_json TEXT NOT NULL DEFAULT '[]',

    time_range TEXT CHECK (time_range IS NULL OR time_range IN ('morning', 'afternoon', 'evening'))
) WITHOUT ROWID;""",
"""CREATE INDEX idx_activity_contentOrigin ON activity(content_origin);""",
"""CREATE INDEX idx_activity_packageOrder ON activity(activity_package_id, order_ind, title, id);""",
"""CREATE INDEX idx_activity_deleteWhenExpired ON activity(delete_when_expired);""",
"""CREATE INDEX idx_activity_readRefStartAt ON activity(sched_read_ref_start_at);""",
"""CREATE UNIQUE INDEX idx_activity_writeRefStartAt_unique ON activity(sched_write_ref_start_at);""",
"""CREATE INDEX idx_activity_readRefRepeatAfterDays ON activity(sched_read_ref_repeat_after_days);""",
"""CREATE UNIQUE INDEX idx_activity_writeRefRepeatAfterDays_unique ON activity(sched_write_ref_repeat_after_days);""",
"""CREATE INDEX idx_activity_readRefDaysDuration ON activity(sched_read_ref_days_duration);""",
"""CREATE UNIQUE INDEX idx_activity_writeRefDaysDuration_unique ON activity(sched_write_ref_days_duration);""",
"""CREATE INDEX idx_activityState_enabled ON activity_state(enabled);""",
"""CREATE INDEX idx_activityState_repeat ON activity_state(repeat);""",
"""CREATE INDEX idx_activityState_monthlyGlanceViewActivityId ON activity_state(monthly_glance_view, activity_id);""",
"""-- Source activity_bundle.sql SHA256 d5ababa357720b89e6ff2069f89e160336fba34c08a43e061e68e35291b5832a
CREATE TABLE activity_bundle (
    id TEXT PRIMARY KEY NOT NULL,

    publisher_id TEXT REFERENCES publisher(id),

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    publisher_title TEXT NOT NULL,

    version INTEGER NOT NULL,

    title TEXT NOT NULL,

    descr TEXT,

    purchase_mode TEXT NOT NULL CHECK (purchase_mode IN ('fullyFree', 'partiallyFree', 'paid')),

    unlock_code TEXT,

    resources_json TEXT NOT NULL,

    icon TEXT NOT NULL,

    promo_image TEXT NOT NULL,

    promo_screens_json TEXT NOT NULL DEFAULT '[]'
) WITHOUT ROWID;""",
"""CREATE TABLE bundle_state (
    bundle_id TEXT PRIMARY KEY NOT NULL REFERENCES activity_bundle(id) ON DELETE CASCADE,
    purchased INTEGER NOT NULL DEFAULT 0 CHECK (purchased IN (0,1)),
    installed_at TEXT
) WITHOUT ROWID;""",
"""CREATE INDEX idx_activityBundle_titleId ON activity_bundle(title, id);""",
"""CREATE INDEX idx_bundleState_installedAt ON bundle_state(installed_at);""",
"""-- Source activity_category.sql SHA256 6eeb6acff08e4e26d5a1731dee1c2b9c915379d5a23000c20811216b179be262
CREATE TABLE activity_category (
    id TEXT PRIMARY KEY NOT NULL,

    title TEXT NOT NULL DEFAULT '',

    icon TEXT NOT NULL
) WITHOUT ROWID;""",
"""-- Source activity_package.sql SHA256 5a69f63be516443a0ea8550b87ae52bbafa792a190817a227bdd9cd163d99f00
CREATE TABLE activity_package (
    id TEXT PRIMARY KEY NOT NULL,

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    activity_bundle_id TEXT NOT NULL REFERENCES activity_bundle(id),

    title TEXT NOT NULL,

    descr TEXT,

    pre_start_text TEXT,

    category_id TEXT NOT NULL REFERENCES activity_category(id),

    icon TEXT NOT NULL
) WITHOUT ROWID;""",
"""CREATE INDEX idx_activityPackage_bundleTitleId ON activity_package(activity_bundle_id, title, id);""",
"""-- Source app_settings.sql SHA256 4d58c4d6b807248c8e4b9f1b27ab18a4511a1f7bcac609b21b5e4a1f50f84e57
CREATE TABLE app_settings (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),

    bootstrapped_date TEXT NOT NULL DEFAULT '',

    most_recent_sync_date TEXT NOT NULL DEFAULT '',

    bootstrap_version INTEGER NOT NULL DEFAULT 0,

    wizard_completed INTEGER NOT NULL DEFAULT 0
);""",
"""-- Source daily_log.sql SHA256 27e265e07d74014da49c79ac4feba549b49aebcb4cec9f4b04acfcc018116905
CREATE TABLE daily_log (
    id TEXT PRIMARY KEY NOT NULL,

    parent_daily_log_id TEXT REFERENCES daily_log(id),

    activity_id TEXT NOT NULL REFERENCES activity(id),

    program_item_id TEXT NOT NULL REFERENCES program_item(id),

    group_id TEXT,

    date TEXT NOT NULL,

    applied_week_ind INTEGER NOT NULL,

    applied_day_ind INTEGER NOT NULL,

    counter INTEGER NOT NULL DEFAULT 0,

    added_manually INTEGER NOT NULL,

    confirmed INTEGER NOT NULL DEFAULT 1,

    inputs_json TEXT NOT NULL DEFAULT '{"values":{}}',

    notes TEXT,

    CHECK (parent_daily_log_id IS NULL OR parent_daily_log_id <> id),
    CHECK (added_manually IN (0,1)),
    CHECK (confirmed IN (0,1))
) WITHOUT ROWID;""",
"""CREATE INDEX idx_dailyLog_programItemId ON daily_log(program_item_id);""",
"""CREATE INDEX idx_dailyLog_date ON daily_log(date);""",
"""CREATE INDEX idx_dailyLog_parentDailyLogId ON daily_log(parent_daily_log_id);""",
"""CREATE INDEX idx_dailyLog_activityDateRange ON daily_log(activity_id, date);""",
"""CREATE INDEX idx_dailyLog_groupDateId ON daily_log(group_id, date, id);""",
"""CREATE INDEX idx_dailyLog_activityConfirmed ON daily_log(activity_id, confirmed);""",
"""-- Source profile.sql SHA256 ce04972ff6bcfccc99ed4a52dbf7b0c936ec1c69d892b93cecbbb8e99726a8a6
CREATE TABLE profile (
    name TEXT PRIMARY KEY NOT NULL,

    measure_system TEXT NOT NULL DEFAULT 'metric' CHECK (measure_system IN ('metric', 'US', 'UK')),

    temperature_system TEXT NOT NULL DEFAULT 'F' CHECK (temperature_system IN ('C', 'F')),

    body_weight INTEGER NOT NULL DEFAULT 0,

    body_weight_last_update TEXT NOT NULL DEFAULT '',

    body_height INTEGER NOT NULL DEFAULT 0,

    body_height_last_update TEXT NOT NULL DEFAULT '',

    unlock_codes TEXT NOT NULL DEFAULT '[]',

    essential_plan INTEGER NOT NULL DEFAULT 0 CHECK (essential_plan IN (0,1)),

    gender TEXT NOT NULL DEFAULT 'male' CHECK (gender IN ('male', 'female', 'other')),

    birthday TEXT NOT NULL DEFAULT '1980-01-01',

    time_zone TEXT NOT NULL DEFAULT 'UTC'
) WITHOUT ROWID;""",
"""-- Source program_item.sql SHA256 006f734ba4d9377ccff17dfcfd3292f7e7c72eb8243f7c331f55cf6b931aa184
CREATE TABLE program_item (
    id TEXT PRIMARY KEY NOT NULL,

    activity_id TEXT NOT NULL REFERENCES activity(id) ON DELETE CASCADE,

    item_id TEXT NOT NULL,

    title TEXT,

    descr TEXT,

    goal_value INTEGER NOT NULL,

    goal_daily_initial INTEGER NOT NULL,

    goal_direction TEXT NOT NULL CHECK (goal_direction IN ('up', 'down')),

    goal_invert INTEGER NOT NULL CHECK (goal_invert IN (0,1)),

    goal_at_least INTEGER NOT NULL CHECK (goal_at_least IN (0,1)),

    goal_single INTEGER NOT NULL CHECK (goal_single IN (0,1)),

    goal_hide_editor INTEGER NOT NULL CHECK (goal_hide_editor IN (0,1)),

    week_index INTEGER NOT NULL CHECK (week_index >= 0),

    day_index INTEGER NOT NULL CHECK (day_index >= 0),

    pre_start_text TEXT,

    post_complete_text TEXT,

    presentation TEXT CHECK (presentation IS NULL OR presentation IN ('default', 'verticalCounterBars')),

    seq_items_json TEXT NOT NULL DEFAULT '[]',

    required_unlock_code TEXT,

    has_unlocked_seq_items INTEGER NOT NULL CHECK (has_unlocked_seq_items IN (0,1)),

    lock_item_display TEXT NOT NULL CHECK (lock_item_display IN ('default', 'blurred')),

    input_entries TEXT NOT NULL DEFAULT '[]'
) WITHOUT ROWID;""",
"""CREATE TABLE program_item_state (
    program_item_id TEXT PRIMARY KEY NOT NULL REFERENCES program_item(id) ON DELETE CASCADE,

    goal_value INTEGER NOT NULL,

    goal_invert INTEGER NOT NULL CHECK (goal_invert IN (0,1)),

    goal_single INTEGER NOT NULL CHECK (goal_single IN (0,1))
) WITHOUT ROWID;""",
"""CREATE TABLE program_item_input_goal (
    id TEXT PRIMARY KEY NOT NULL,

    program_item_id TEXT NOT NULL REFERENCES program_item(id) ON DELETE CASCADE,

    input_key TEXT NOT NULL,

    current_revision INTEGER NOT NULL CHECK (current_revision > 0),

    UNIQUE(program_item_id, input_key),
    CHECK (input_key <> '')
) WITHOUT ROWID;""",
"""CREATE TABLE program_item_input_goal_revision (
    id TEXT PRIMARY KEY NOT NULL,

    goal_id TEXT NOT NULL REFERENCES program_item_input_goal(id) ON DELETE CASCADE,

    revision INTEGER NOT NULL CHECK (revision > 0),

    effective_day TEXT NOT NULL CHECK (
        length(effective_day) = 10
        AND substr(effective_day, 5, 1) = '-'
        AND substr(effective_day, 8, 1) = '-'
    ),

    goal_kind TEXT NOT NULL CHECK (goal_kind IN ('none', 'range', 'upper_limit', 'lower_limit')),

    policy_json TEXT NOT NULL DEFAULT '{}',

    UNIQUE(goal_id, revision)
) WITHOUT ROWID;""",
"""CREATE INDEX idx_programItem_activityWeekDayId ON program_item(activity_id, week_index, day_index, id);""",
"""CREATE INDEX idx_programItemInputGoal_programItem
    ON program_item_input_goal(program_item_id, input_key);""",
"""CREATE INDEX idx_programItemInputGoalRevision_effective
    ON program_item_input_goal_revision(goal_id, effective_day DESC, revision DESC);""",
"""-- Source publisher.sql SHA256 d106ddb400f8aa68094cea22753b6dd64209aeda6d99b26dfa91658802113398
CREATE TABLE publisher (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL
) WITHOUT ROWID;""",
"""-- Source value_registry.sql SHA256 2d953792b643fd8d169c03083186d115c44c3f56b65f7a31619638bbb726fc72
CREATE TABLE value_registry (
    key TEXT PRIMARY KEY NOT NULL,

    value_type TEXT NOT NULL CHECK (value_type IN ('null', 'string', 'numeric', 'date')),

    updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00.000000000Z',

    string_value TEXT,

    numeric_value REAL
) WITHOUT ROWID;""",
"""CREATE INDEX idx_valueRegistry_valueType ON value_registry(value_type);""",
"""CREATE INDEX idx_valueRegistry_updatedAt ON value_registry(updated_at);"""
    )
    private val seedStatements = listOf(
"""INSERT INTO publisher VALUES('publisher','Synthetic publisher');""",
"""INSERT INTO activity_category VALUES('category','Synthetic category','{"icon":"test"}');""",
"""INSERT INTO activity_bundle(id,publisher_id,content_origin,publisher_title,version,title,descr,purchase_mode,resources_json,icon,promo_image) VALUES('bundle','publisher','user','Publisher',1,'Bundle','description','fullyFree','{}','{}','{}');""",
"""INSERT INTO bundle_state VALUES('bundle',1,'2026-10-01T00:00:00.000000000Z');""",
"""INSERT INTO activity_package(id,content_origin,activity_bundle_id,title,descr,category_id,icon) VALUES('package','user','bundle','Package','description','category','{}');""",
"""INSERT INTO activity(id,content_origin,group_id,activity_package_id,category_id,program_type,delete_when_expired,days_confirm_required,order_ind,title,descr,icon,sched_repeat,sched_repeat_after_days,sched_days_duration,sched_write_ref_start_at,sched_write_ref_repeat_after_days,sched_write_ref_days_duration) VALUES
('A','user','group','package','category','finite',1,0,0,'Activity A','description','{}','daysInterval',1,2,'date-a','repeat-a','duration-a'),
('B','user','group','package','category','finite',0,1,1,'Activity B','description','{}','none',0,0,'date-b','repeat-b','duration-b'),
('C','user','group','package','category','simple',0,0,2,'Activity C','description','{}','none',0,0,NULL,NULL,NULL),
('D','user','group','package','category','marker',0,0,3,'Activity D','description','{}','none',0,0,NULL,NULL,NULL);""",
"""INSERT INTO activity_state(activity_id,enabled,monthly_glance_view,category_id,program_type,repeat,repeat_after_days,days_duration) VALUES('A',1,0,'category','finite','daysInterval',1,2);""",
"""INSERT INTO program_item(id,activity_id,item_id,title,goal_value,goal_daily_initial,goal_direction,goal_invert,goal_at_least,goal_single,goal_hide_editor,week_index,day_index,has_unlocked_seq_items,lock_item_display) VALUES
('P','A','item-p','Program P',7,1,'up',0,1,0,0,0,0,1,'default'),
('Q','B','item-q','Program Q',3,1,'down',1,0,1,0,1,2,0,'blurred');""",
"""INSERT INTO program_item_state VALUES('P',7,0,1),('Q',3,1,0);""",
"""INSERT INTO program_item_input_goal VALUES('G','P','weight',1),('H','P','reps',1);""",
"""INSERT INTO program_item_input_goal_revision VALUES('R','G',1,'2026-10-01','range','{"min":2.5}'),('S','G',2,'2026-10-02','upper_limit','{"max":10}'),('T','H',1,'2026-10-01','none','{}');""",
"""INSERT INTO daily_log(id,parent_daily_log_id,activity_id,program_item_id,date,applied_week_ind,applied_day_ind,added_manually,confirmed,notes) VALUES
('L1',NULL,'A','P','2026-10-01',0,0,0,1,'parent note'),('L2','L1','A','P','2026-10-02',0,1,1,0,'child note');""",
"""INSERT INTO profile(name,measure_system,temperature_system,body_weight,body_height,essential_plan,gender,birthday,time_zone) VALUES('profile-name','metric','C',73,180,1,'other','1980-01-01','Europe/Bucharest');""",
"""INSERT INTO value_registry VALUES('body-weight','numeric','2026-10-01T00:00:00.123456789Z',NULL,73.25),('json-text','string','2026-10-01T00:00:00.000000000Z','{"text":"kept as text"}',NULL);""",
"""INSERT INTO app_settings VALUES(1,'2026-10-01','2026-10-02',7,1);"""
    )
}
