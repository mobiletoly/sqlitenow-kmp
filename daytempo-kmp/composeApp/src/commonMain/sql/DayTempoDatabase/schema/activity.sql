CREATE TABLE activity (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    depends_on_doc_id TEXT,

    group_doc_id TEXT NOT NULL,

    activity_bundle_doc_id TEXT NOT NULL REFERENCES activity_bundle(doc_id),

    activity_package_doc_id TEXT NOT NULL REFERENCES activity_package(doc_id),

    -- @@{ field=deleted, propertyType=kotlin.Boolean }
    deleted INTEGER NOT NULL CHECK (deleted IN (0,1)),

    -- @@{ field=enabled, propertyType=kotlin.Boolean }
    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),

    -- @@{ field=user_defined, propertyType=kotlin.Boolean }
    user_defined INTEGER NOT NULL CHECK (user_defined IN (0,1)),

    -- @@{ field=program_type, propertyType=com.pluralfusion.daytempo.domain.model.ActivityProgramType }
    program_type TEXT NOT NULL,

    -- @@{ field=delete_when_expired, propertyType=kotlin.Boolean }
    delete_when_expired INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=days_confirm_required, propertyType=kotlin.Boolean }
    days_confirm_required INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=order_ind, propertyType=kotlin.Int }
    order_ind INTEGER NOT NULL CHECK (order_ind >= 0),

    -- @@{ field=installed_at, propertyType=kotlinx.datetime.LocalDateTime }
    installed_at INTEGER,

    title TEXT NOT NULL,

    descr TEXT NOT NULL,

    category_doc_id TEXT NOT NULL REFERENCES activity_category(doc_id),

    -- @@{ field=icon, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    icon TEXT NOT NULL,

    -- @@{ field=monthly_glance_view, propertyType=kotlin.Boolean }
    monthly_glance_view INTEGER NOT NULL CHECK (monthly_glance_view IN (0,1)),

    required_unlock_code TEXT,

    -- @@{ field=priority, propertyType=kotlin.Int }
    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority >= 0),

    -- @@{ field=unlocked_days, propertyType=kotlin.Int }
    unlocked_days INTEGER,

    -- @@{ field=reporting, propertyType=com.pluralfusion.daytempo.domain.model.ActivityReportingType }
    reporting TEXT NOT NULL DEFAULT 'default',

    --- SCHEDULE ---

    -- @@{ field=sched_mandatory_to_setup, propertyType=kotlin.Boolean }
    sched_mandatory_to_setup INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=sched_repeat, propertyType=com.pluralfusion.daytempo.domain.model.ActivityScheduleRepeat }
    sched_repeat TEXT NOT NULL,

    -- @@{ field=sched_allowed_repeat_modes, propertyType=kotlin.collections.Set<com.pluralfusion.daytempo.domain.model.ActivityScheduleRepeat> }
    sched_allowed_repeat_modes TEXT NOT NULL,

    -- @@{ field=sched_mon, propertyType=kotlin.Boolean }
    sched_mon INTEGER NOT NULL,

    -- @@{ field=sched_tue, propertyType=kotlin.Boolean }
    sched_tue INTEGER NOT NULL,

    -- @@{ field=sched_wed, propertyType=kotlin.Boolean }
    sched_wed INTEGER NOT NULL,

    -- @@{ field=sched_thu, propertyType=kotlin.Boolean }
    sched_thu INTEGER NOT NULL,

    -- @@{ field=sched_fri, propertyType=kotlin.Boolean }
    sched_fri INTEGER NOT NULL,

    -- @@{ field=sched_sat, propertyType=kotlin.Boolean }
    sched_sat INTEGER NOT NULL,

    -- @@{ field=sched_sun, propertyType=kotlin.Boolean }
    sched_sun INTEGER NOT NULL,

    -- @@{ field=sched_week1, propertyType=kotlin.Boolean }
    sched_week1 INTEGER NOT NULL,

    -- @@{ field=sched_week2, propertyType=kotlin.Boolean }
    sched_week2 INTEGER NOT NULL,

    -- @@{ field=sched_week3, propertyType=kotlin.Boolean }
    sched_week3 INTEGER NOT NULL,

    -- @@{ field=sched_week4, propertyType=kotlin.Boolean }
    sched_week4 INTEGER NOT NULL,

    -- @@{ field=sched_day0, propertyType=kotlin.Int }
    sched_day0 INTEGER NOT NULL,

    -- @@{ field=sched_day1, propertyType=kotlin.Int }
    sched_day1 INTEGER NOT NULL,

    -- @@{ field=sched_day2, propertyType=kotlin.Int }
    sched_day2 INTEGER NOT NULL,

    -- @@{ field=sched_day3, propertyType=kotlin.Int }
    sched_day3 INTEGER NOT NULL,

    -- @@{ field=sched_day4, propertyType=kotlin.Int }
    sched_day4 INTEGER NOT NULL,

    -- @@{ field=sched_start_at, propertyType=kotlinx.datetime.LocalDate, notNull=false }
    sched_start_at INTEGER NOT NULL,

    -- @@{ field=sched_start_at_eval }
    sched_start_at_eval TEXT,

    -- @@{ field=sched_read_ref_start_at }
    sched_read_ref_start_at TEXT,

    -- @@{ field=sched_write_ref_start_at }
    sched_write_ref_start_at TEXT,

    -- @@{ field=sched_start_at_label }
    sched_start_at_label TEXT,

    -- @@{ field=sched_repeat_after_days, propertyType=kotlin.Int }
    sched_repeat_after_days INTEGER NOT NULL,

    -- @@{ field=sched_read_ref_repeat_after_days }
    sched_read_ref_repeat_after_days TEXT,

    -- @@{ field=sched_write_ref_repeat_after_days }
    sched_write_ref_repeat_after_days TEXT,

    -- @@{ field=sched_repeat_after_days_label }
    sched_repeat_after_days_label TEXT,

    -- @@{ field=sched_repeat_after_days_min, propertyType=kotlin.Int }
    sched_repeat_after_days_min INTEGER,

    -- @@{ field=sched_repeat_after_days_max, propertyType=kotlin.Int }
    sched_repeat_after_days_max INTEGER,

    -- @@{ field=sched_allow_edit_days_duration, propertyType=kotlin.Boolean }
    sched_allow_edit_days_duration INTEGER NOT NULL,

    -- @@{ field=sched_days_duration, propertyType=kotlin.Int }
    sched_days_duration INTEGER NOT NULL,

    -- @@{ field=sched_read_ref_days_duration }
    sched_read_ref_days_duration TEXT,

    -- @@{ field=sched_write_ref_days_duration }
    sched_write_ref_days_duration TEXT,

    -- @@{ field=sched_days_duration_label }
    sched_days_duration_label TEXT,

    -- @@{ field=sched_days_duration_min, propertyType=kotlin.Int }
    sched_days_duration_min INTEGER,

    -- @@{ field=sched_days_duration_max, propertyType=kotlin.Int }
    sched_days_duration_max INTEGER,

    -- @@{ field=sched_time_points, propertyType=kotlin.collections.List<com.pluralfusion.daytempo.domain.model.AlarmHourMinute> }
    sched_time_points TEXT NOT NULL,

    -- @@{ field=sched_time_range, propertyType=com.pluralfusion.daytempo.domain.model.ActivityScheduleTimeRange }
    sched_time_range TEXT
) WITHOUT ROWID;

CREATE INDEX idx_activity_dependsOnDocId ON activity(depends_on_doc_id);
CREATE INDEX idx_activity_bundleDocId  ON activity(activity_bundle_doc_id);
CREATE INDEX idx_activity_packageDocId ON activity(activity_package_doc_id);
CREATE INDEX idx_activity_enabled ON activity(enabled);
CREATE INDEX idx_activity_deleted ON activity(deleted);
CREATE INDEX idx_activity_userDefined ON activity(user_defined);
CREATE INDEX idx_activity_programType ON activity(program_type);
CREATE INDEX idx_activity_daysConfirmRequired ON activity(days_confirm_required);
CREATE INDEX idx_activity_deleteWhenExpired ON activity(delete_when_expired);
CREATE INDEX idx_activity_categoryDocId ON activity(category_doc_id);
CREATE INDEX idx_activity_monthlyGlanceView ON activity(monthly_glance_view);
CREATE INDEX idx_activity_requiredUnlockCode ON activity(required_unlock_code);

-- Activity with with category and schedule
CREATE VIEW activity_detailed_view AS
SELECT
    act.id AS act__id,
    act.doc_id AS act__doc_id,
    act.depends_on_doc_id AS act__depends_on_doc_id,
    act.group_doc_id AS act__group_doc_id,
    act.activity_bundle_doc_id AS act__activity_bundle_doc_id,
    act.activity_package_doc_id AS act__activity_package_doc_id,
    act.deleted AS act__deleted,
    act.enabled AS act__enabled,
    act.user_defined AS act__user_defined,
    act.program_type AS act__program_type,
    act.delete_when_expired AS act__delete_when_expired,
    act.days_confirm_required AS act__days_confirm_required,
    act.order_ind AS act__order_ind,
    act.installed_at AS act__installed_at,
    act.title AS act__title,
    act.descr AS act__descr,
    act.category_doc_id AS act__category_doc_id,
    act.icon AS act__icon,
    act.monthly_glance_view AS act__monthly_glance_view,
    act.required_unlock_code AS act__required_unlock_code,
    act.priority AS act__priority,
    act.unlocked_days AS act__unlocked_days,
    act.reporting AS act__reporting,
    act.sched_mandatory_to_setup AS act__sched_mandatory_to_setup,
    act.sched_repeat AS act__sched_repeat,
    act.sched_allowed_repeat_modes AS act__sched_allowed_repeat_modes,
    act.sched_mon AS act__sched_mon,
    act.sched_tue AS act__sched_tue,
    act.sched_wed AS act__sched_wed,
    act.sched_thu AS act__sched_thu,
    act.sched_fri AS act__sched_fri,
    act.sched_sat AS act__sched_sat,
    act.sched_sun AS act__sched_sun,
    act.sched_week1 AS act__sched_week1,
    act.sched_week2 AS act__sched_week2,
    act.sched_week3 AS act__sched_week3,
    act.sched_week4 AS act__sched_week4,
    act.sched_day0 AS act__sched_day0,
    act.sched_day1 AS act__sched_day1,
    act.sched_day2 AS act__sched_day2,
    act.sched_day3 AS act__sched_day3,
    act.sched_day4 AS act__sched_day4,
    act.sched_start_at AS act__sched_start_at,
    act.sched_start_at_eval AS act__sched_start_at_eval,
    act.sched_read_ref_start_at AS act__sched_read_ref_start_at,
    act.sched_write_ref_start_at AS act__sched_write_ref_start_at,
    act.sched_start_at_label AS act__sched_start_at_label,
    act.sched_repeat_after_days AS act__sched_repeat_after_days,
    act.sched_read_ref_repeat_after_days AS act__sched_read_ref_repeat_after_days,
    act.sched_write_ref_repeat_after_days AS act__sched_write_ref_repeat_after_days,
    act.sched_repeat_after_days_label AS act__sched_repeat_after_days_label,
    act.sched_repeat_after_days_min AS act__sched_repeat_after_days_min,
    act.sched_repeat_after_days_max AS act__sched_repeat_after_days_max,
    act.sched_allow_edit_days_duration AS act__sched_allow_edit_days_duration,
    act.sched_days_duration AS act__sched_days_duration,
    act.sched_read_ref_days_duration AS act__sched_read_ref_days_duration,
    act.sched_write_ref_days_duration AS act__sched_write_ref_days_duration,
    act.sched_days_duration_label AS act__sched_days_duration_label,
    act.sched_days_duration_min AS act__sched_days_duration_min,
    act.sched_days_duration_max AS act__sched_days_duration_max,
    act.sched_time_points AS act__sched_time_points,
    act.sched_time_range AS act__sched_time_range,

    cat.id AS joined__act__category__id,
    cat.doc_id AS joined__act__category__doc_id,
    cat.title AS joined__act__category__title,
    cat.icon AS joined__act__category__icon,

    -- Pick ONE program_item per activity
    -- @@{ field=first_program_item_doc_id, propertyType=kotlin.String, notNull=true }
    (SELECT pi.doc_id
     FROM program_item AS pi
     WHERE pi.activity_doc_id = act.id
     ORDER BY pi.doc_id                  -- choose the column(s) that define "first" (TODO)
     LIMIT 1) AS first_program_item_doc_id

  /* @@{ dynamicField=main,
         mappingType=entity,
         propertyType=ActivityRow,
         sourceTable=act,
         aliasPrefix=act__,
         notNull=true } */

  /* @@{ dynamicField=category,
         mappingType=perRow,
         propertyType=ActivityCategoryRow,
         sourceTable=cat,
         aliasPrefix=joined__act__category__
         notNull=true } */

FROM activity act
    LEFT JOIN activity_category cat ON act.category_doc_id = cat.doc_id;

-- Activity with program items
-- @@{ collectionKey=act__doc_id }
CREATE VIEW activity_with_program_items_view AS
SELECT
    av.*,
    pi.*

  /* @@{ dynamicField=main,
         mappingType=entity,
         propertyType=ActivityDetailedRow,
         sourceTable=av,
         aliasPrefix=act__,
         notNull=true } */

  /* @@{ dynamicField=programItems,
         mappingType=collection,
         propertyType=List<ProgramItemRow>,
         sourceTable=pi,
         collectionKey=pi__doc_id,
         aliasPrefix=pi__
         notNull=true } */

FROM activity_detailed_view av
    LEFT JOIN program_item_to_join pi ON av.act__doc_id = pi.pi__activity_doc_id;
