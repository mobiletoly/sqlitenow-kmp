CREATE TABLE activity_package (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    activity_bundle_doc_id TEXT NOT NULL,

    title TEXT NOT NULL,

    descr TEXT,

    pre_start_text TEXT,

    -- @@{ field=user_defined, propertyType=kotlin.Boolean }
    user_defined INTEGER NOT NULL,

    category_doc_id TEXT NOT NULL,

    -- @@{ field=icon, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    icon TEXT NOT NULL,

    FOREIGN KEY (activity_bundle_doc_id) REFERENCES activity_bundle(doc_id),
    FOREIGN KEY (category_doc_id) REFERENCES activity_category(doc_id)
) WITHOUT ROWID;

CREATE INDEX idx_activityPackage_activityBundleDocId ON activity_package (activity_bundle_doc_id);
CREATE INDEX idx_activityPackage_userDefined ON activity_package (user_defined);
CREATE INDEX idx_activityPackage_categoryDocId ON activity_package (category_doc_id);

-- Activity package for join operations
CREATE VIEW activity_package_to_join AS
SELECT
    pkg.id AS package__id,
    pkg.doc_id AS package__doc_id,
    pkg.activity_bundle_doc_id AS package__activity_bundle_doc_id,
    pkg.title AS package__title,
    pkg.descr AS package__descr,
    pkg.pre_start_text AS package__pre_start_text,
    pkg.user_defined AS package__user_defined,
    pkg.category_doc_id AS package__category_doc_id,
    pkg.icon AS package__icon
FROM activity_package AS pkg;

-- Activity package with category
CREATE VIEW activity_package_detailed_view AS
SELECT
    pkg.*,

    cat.id AS joined__package__category__id,
    cat.doc_id AS joined__package__category__doc_id,
    cat.title AS joined__package__category__title,
    cat.icon AS joined__package__category__icon,

    -- @@{ field=act_summary__count, propertyType=kotlin.Int, notNull=true }
    (
      SELECT COUNT(DISTINCT act.doc_id)
      FROM activity AS act
      WHERE act.activity_package_doc_id = pkg.package__doc_id
    ) AS act_summary__count,

    -- @@{ field=act_summary__titles, propertyType=kotlin.collections.Set<kotlin.String>, sqlTypeHint=TEXT, notNull=true }
    CAST((
          SELECT GROUP_CONCAT(t.title, CHAR(9) ORDER BY t.title COLLATE NOCASE)
          FROM (
            SELECT DISTINCT act.title
            FROM activity AS act
            WHERE act.activity_package_doc_id = pkg.package__doc_id
              AND act.enabled = 1
              AND act.deleted = 0
              AND act.title IS NOT NULL
          ) AS t
    ) AS TEXT) AS act_summary__titles,

    -- @@{ field=act_summary__statuses, propertyType=kotlin.collections.Set<com.pluralfusion.daytempo.domain.model.ActivityStatus>, sqlTypeHint=TEXT, notNull=true }
    CAST((
      SELECT GROUP_CONCAT(s.status, CHAR(9) ORDER BY s.status)
      FROM (
        SELECT DISTINCT 'SETUP_REQ' AS status
        FROM activity AS act
        WHERE act.activity_package_doc_id = pkg.package__doc_id
          AND act.deleted = 0
          AND act.sched_mandatory_to_setup = 1
          AND act.sched_repeat = 'daysInterval'
          AND act.sched_start_at = 0
          AND act.sched_start_at_eval IS NULL
      ) AS s
    ) AS TEXT) AS act_summary__statuses,

    -- @@{ field=act_summary__are_all_group_doc_ids_same, propertyType=kotlin.Boolean, notNull=true }
    (
      SELECT CASE
               WHEN COUNT(*) = 0 THEN 0
               WHEN COUNT(DISTINCT act.group_doc_id) = 1 THEN 1
               ELSE 0
             END
      FROM activity AS act
      WHERE act.activity_package_doc_id = pkg.package__doc_id
    ) AS act_summary__are_all_group_doc_ids_same

    /* @@{ dynamicField=main,
         mappingType=entity,
         propertyType=ActivityPackageRow,
         sourceTable=pkg,
         aliasPrefix=package__,
         notNull=true } */

    /* @@{ dynamicField=category,
         mappingType=perRow,
         propertyType=ActivityCategoryRow,
         sourceTable=cat,
         aliasPrefix=joined__package__category__
         notNull=true } */

    /* @@{ dynamicField=activitySummary,
         mappingType=perRow,
         propertyType=com.pluralfusion.daytempo.domain.model.PackageActivitySummary,
         sourceTable=act_summary,
         aliasPrefix=act_summary__
         notNull=true } */

FROM activity_package_to_join pkg
    LEFT JOIN activity_category AS cat
    ON pkg.package__category_doc_id = cat.doc_id;


-- Activity package with activities
-- @@{ collectionKey=package__doc_id }
CREATE VIEW activity_package_with_activities_view AS
SELECT
    pkg.*,
    act.*

    /* @@{ dynamicField=activities,
           mappingType=collection,
           propertyType=List<ActivityDetailedRow>,
           sourceTable=act,
           collectionKey=act__id,
           aliasPrefix=act__,
           notNull=true } */

FROM activity_package_detailed_view pkg
    LEFT JOIN activity_detailed_view act ON pkg.package__doc_id = act.act__activity_package_doc_id;
