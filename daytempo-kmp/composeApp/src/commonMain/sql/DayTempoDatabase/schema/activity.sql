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
    reporting TEXT NOT NULL DEFAULT 'default'
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

    sch.*,

    cat.id AS joined__act__category__id,
    cat.doc_id AS joined__act__category__doc_id,
    cat.title AS joined__act__category__title,
    cat.icon AS joined__act__category__icon

  /* @@{ dynamicField=main,
         mappingType=entity,
         propertyType=ActivityDoc,
         sourceTable=act,
         aliasPrefix=act__,
         notNull=true } */

  /* @@{ dynamicField=category,
         mappingType=perRow,
         propertyType=ActivityCategoryDoc,
         sourceTable=cat,
         aliasPrefix=joined__act__category__
         notNull=true } */

  /* @@{ dynamicField=schedule,
         mappingType=perRow,
         propertyType=ActivityScheduleDoc,
         sourceTable=sch,
         aliasPrefix=schedule__
         notNull=true } */

FROM activity act
    LEFT JOIN activity_category cat ON act.category_doc_id = cat.doc_id
    LEFT JOIN activity_schedule_to_join sch ON act.id = sch.schedule__activity_id;

-- Activity with program items
-- @@{ collectionKey=act__doc_id }
CREATE VIEW activity_with_program_items_view AS
SELECT
    av.*,
    pi.*

  /* @@{ dynamicField=programItems,
         mappingType=collection,
         propertyType=List<ProgramItemDoc>,
         sourceTable=pi,
         collectionKey=pi__doc_id,
         aliasPrefix=pi__
         notNull=true } */

FROM activity_detailed_view av
    LEFT JOIN program_item_to_join pi ON av.act__doc_id = pi.pi__activity_doc_id;
