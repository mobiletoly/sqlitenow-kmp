CREATE TABLE daily_log (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    parent_daily_log_doc_id TEXT REFERENCES daily_log(doc_id),

    activity_doc_id TEXT NOT NULL REFERENCES activity(doc_id),

    program_item_doc_id TEXT NOT NULL REFERENCES program_item(doc_id),

    group_doc_id TEXT,

    -- @@{ field=days_since_epoch, propertyType=kotlin.Int }
    days_since_epoch INTEGER NOT NULL,

    -- @@{ field=year, propertyType=kotlin.Int }
    year INTEGER NOT NULL,

    -- @@{ field=month, propertyType=kotlin.Int }
    month INTEGER NOT NULL,

    -- @@{ field=day, propertyType=kotlin.Int }
    day INTEGER NOT NULL,

    -- @@{ field=applied_week_ind, propertyType=kotlin.Int }
    applied_week_ind INTEGER NOT NULL,

    -- @@{ field=applied_day_ind, propertyType=kotlin.Int }
    applied_day_ind INTEGER NOT NULL,

    -- @@{ field=counter, propertyType=kotlin.Int }
    counter INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=added_manually, propertyType=kotlin.Boolean }
    added_manually INTEGER NOT NULL,

    -- @@{ field=confirmed, propertyType=kotlin.Boolean }
    confirmed INTEGER NOT NULL DEFAULT 1,

    numeric_value00 REAL,

    numeric_value01 REAL,

    numeric_value02 REAL,

    numeric_value03 REAL,

    numeric_value04 REAL,

    numeric_value05 REAL,

    numeric_value06 REAL,

    numeric_value07 REAL,

    numeric_value08 REAL,

    numeric_value09 REAL,

    string_value00 TEXT,

    string_value01 TEXT,

    string_value02 TEXT,

    string_value03 TEXT,

    string_value04 TEXT,

    string_value05 TEXT,

    string_value06 TEXT,

    string_value07 TEXT,

    string_value08 TEXT,

    string_value09 TEXT,

    notes TEXT,

    CHECK (parent_daily_log_doc_id IS NULL OR parent_daily_log_doc_id <> doc_id),  -- avoid self-loop
    CHECK (added_manually IN (0,1)),
    CHECK (confirmed IN (0,1))
) WITHOUT ROWID;

CREATE INDEX idx_dailyLog_activityDocId ON daily_log(activity_doc_id);
CREATE INDEX idx_dailyLog_programItemDocId ON daily_log(program_item_doc_id);
CREATE INDEX idx_dailyLog_date ON daily_log(year, month, day);
CREATE INDEX idx_dailyLog_daysSinceEpoch ON daily_log(days_since_epoch);
CREATE INDEX idx_dailyLog_parentDailyLogDocId ON daily_log(parent_daily_log_doc_id);
CREATE INDEX idx_dailyLog_groupDocId ON daily_log(group_doc_id);
CREATE INDEX idx_dailyLog_confirmed ON daily_log(confirmed);
CREATE INDEX idx_dailyLog_activityDateRange ON daily_log(activity_doc_id, days_since_epoch);

CREATE VIEW daily_log_for_join AS
SELECT
    id AS dl__id,
    doc_id AS dl__doc_id,
    parent_daily_log_doc_id AS dl__parent_daily_log_doc_id,
    group_doc_id AS dl__group_doc_id,
    activity_doc_id AS dl__activity_doc_id,
    program_item_doc_id AS dl__program_item_doc_id,
    confirmed AS dl__confirmed,
    days_since_epoch AS dl__days_since_epoch,
    year AS dl__year,
    month AS dl__month,
    day AS dl__day,
    applied_week_ind AS dl__applied_week_ind,
    applied_day_ind AS dl__applied_day_ind,
    counter AS dl__counter,
    added_manually AS dl__added_manually,
    numeric_value00 AS dl__numeric_value00,
    numeric_value01 AS dl__numeric_value01,
    numeric_value02 AS dl__numeric_value02,
    numeric_value03 AS dl__numeric_value03,
    numeric_value04 AS dl__numeric_value04,
    numeric_value05 AS dl__numeric_value05,
    numeric_value06 AS dl__numeric_value06,
    numeric_value07 AS dl__numeric_value07,
    numeric_value08 AS dl__numeric_value08,
    numeric_value09 AS dl__numeric_value09,
    string_value00 AS dl__string_value00,
    string_value01 AS dl__string_value01,
    string_value02 AS dl__string_value02,
    string_value03 AS dl__string_value03,
    string_value04 AS dl__string_value04,
    string_value05 AS dl__string_value05,
    string_value06 AS dl__string_value06,
    string_value07 AS dl__string_value07,
    string_value08 AS dl__string_value08,
    string_value09 AS dl__string_value09,
    notes AS dl__notes
FROM daily_log;

CREATE VIEW daily_log_detailed_view AS
SELECT
    dl.*,
    act.*,
    cat.*,
    pi.*

  /* @@{ dynamicField=dailyLog,
         mappingType=entity,
         propertyType=DailyLogDoc,
         sourceTable=dl,
         aliasPrefix=dl__,
         notNull=true } */

  /* @@{ dynamicField=activity,
         mappingType=entity,
         propertyType=ActivityDoc,
         sourceTable=act,
         aliasPrefix=act__,
         notNull=true } */

   /* @@{ dynamicField=category,
         mappingType=perRow,
         propertyType=ActivityCategoryDoc,
         sourceTable=cat,
         aliasPrefix=category__
         notNull=true } */

    /* @@{ dynamicField=programItem,
         mappingType=perRow,
         propertyType=ProgramItemDoc,
         sourceTable=pi,
         aliasPrefix=pi__
         notNull=true } */

FROM daily_log_for_join dl
    LEFT JOIN activity_for_join act ON dl.dl__activity_doc_id = act.act__doc_id
    LEFT JOIN activity_category_to_join cat ON act.act__category_doc_id = cat.category__doc_id
    LEFT JOIN program_item_to_join pi ON dl.dl__program_item_doc_id = pi.pi__doc_id;
