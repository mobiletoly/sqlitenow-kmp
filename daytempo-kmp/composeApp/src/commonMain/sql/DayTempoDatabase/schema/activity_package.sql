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
    cat.*

  /* @@{ dynamicField=main,
         mappingType=entity,
         propertyType=ActivityPackageDoc,
         sourceTable=pkg,
         aliasPrefix=package__,
         notNull=true } */

  /* @@{ dynamicField=category,
         mappingType=perRow,
         propertyType=ActivityCategoryDoc,
         sourceTable=cat,
         aliasPrefix=category__
         notNull=true } */

FROM activity_package_to_join pkg
    LEFT JOIN activity_category_to_join cat ON pkg.package__category_doc_id = cat.category__doc_id;


-- Activity package with activities
-- @@{ collectionKey=package__doc_id }
CREATE VIEW activity_package_with_activities_view AS
SELECT
    pkg.*,
    act.*

    /* @@{ dynamicField=activities,
           mappingType=collection,
           propertyType=List<ActivityDetailedDoc>,
           sourceTable=act,
           collectionKey=act__id,
           aliasPrefix=act__,
           notNull=true } */

FROM activity_package_detailed_view pkg
    LEFT JOIN activity_detailed_view act ON pkg.package__doc_id = act.act__activity_package_doc_id;
