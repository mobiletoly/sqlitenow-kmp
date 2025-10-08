CREATE TABLE activity_bundle (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    provider_doc_id TEXT NOT NULL,

    -- @@{ field=version, propertyType=kotlin.Int }
    version INTEGER NOT NULL,

    title TEXT NOT NULL,

    descr TEXT,

    -- @@{ field=user_defined, propertyType=kotlin.Boolean }
    user_defined INTEGER NOT NULL,

    -- @@{ field=purchase_mode, propertyType=com.pluralfusion.daytempo.domain.model.ActivityBundlePurchaseMode }
    purchase_mode TEXT NOT NULL,

    unlock_code TEXT,

    -- @@{ field=purchased, propertyType=kotlin.Boolean }
    purchased INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=installed_at, propertyType=kotlinx.datetime.LocalDateTime }
    installed_at INTEGER,

    resources_json TEXT NOT NULL,

    -- @@{ field=icon, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    icon TEXT NOT NULL,

    -- @@{ field=promo_image, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    promo_image TEXT NOT NULL,

    -- @@{ field=promo_scr1, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    promo_scr1 TEXT,

    -- @@{ field=promo_scr2, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    promo_scr2 TEXT,

    -- @@{ field=promo_scr3, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    promo_scr3 TEXT,

    FOREIGN KEY (provider_doc_id) REFERENCES provider(doc_id)
) WITHOUT ROWID;

CREATE INDEX idx_activityBundle_providerDocId ON activity_bundle (provider_doc_id);
CREATE INDEX idx_activityBundle_version ON activity_bundle (version);
CREATE INDEX idx_activityBundle_userDefined ON activity_bundle (user_defined);
CREATE INDEX idx_activityBundle_purchaseMode ON activity_bundle (purchase_mode);
CREATE INDEX idx_activityBundle_installedAt ON activity_bundle (installed_at);
CREATE INDEX idx_activityBundle_purchased ON activity_bundle (purchased);


-- Activity bundle for join operations
CREATE VIEW activity_bundle_to_join AS
SELECT
    bndl.id AS bundle__id,
    bndl.doc_id AS bundle__doc_id,
    bndl.provider_doc_id AS bundle__provider_doc_id,
    bndl.version AS bundle__version,
    bndl.title AS bundle__title,
    bndl.descr AS bundle__descr,
    bndl.user_defined AS bundle__user_defined,
    bndl.purchase_mode AS bundle__purchase_mode,
    bndl.unlock_code AS bundle__unlock_code,
    bndl.purchased AS bundle__purchased,
    bndl.installed_at AS bundle__installed_at,
    bndl.resources_json AS bundle__resources_json,
    bndl.icon AS bundle__icon,
    bndl.promo_image AS bundle__promo_image,
    bndl.promo_scr1 AS bundle__promo_scr1,
    bndl.promo_scr2 AS bundle__promo_scr2,
    bndl.promo_scr3 AS bundle__promo_scr3
FROM activity_bundle AS bndl;


-- Activity bundle with provider and category
CREATE VIEW activity_bundle_detailed_view AS
SELECT
    bndl.*,
    prov.*,

    cat.id AS joined__bndl__category__id,
    cat.doc_id AS joined__bndl__category__doc_id,
    cat.title AS joined__bndl__category__title,
    cat.icon AS joined__bndl__category__icon

    /* @@{ dynamicField=bndl,
           mappingType=entity,
           propertyType=ActivityBundleRow,
           sourceTable=bndl,
           aliasPrefix=bundle__
           notNull=true } */

    /* @@{ dynamicField=provider,
           mappingType=perRow,
           propertyType=ProviderRow,
           sourceTable=prov,
           aliasPrefix=provider__
           notNull=true } */

    /* @@{ dynamicField=category,
           mappingType=perRow,
           propertyType=ActivityCategoryRow,
           sourceTable=cat,
           aliasPrefix=joined__bndl__category__
           notNull=true } */

FROM activity_bundle_to_join bndl
    LEFT JOIN provider_to_join prov ON bndl.bundle__provider_doc_id = prov.provider__doc_id
    LEFT JOIN activity_package_to_join pkg ON bndl.bundle__doc_id = pkg.package__activity_bundle_doc_id
    LEFT JOIN activity_category cat ON pkg.package__category_doc_id = cat.doc_id;


-- Detailed activity bundle with packages
-- @@{ collectionKey=bundle__doc_id }
CREATE VIEW activity_bundle_with_packages_view AS
SELECT
    bndl.*,
    pkg.*

    /* @@{ dynamicField=bndlDetailed,
           mappingType=entity,
           propertyType=ActivityBundleDetailedRow,
           sourceTable=bndl,
           aliasPrefix=bundle__
           notNull=true } */

    /* @@{ dynamicField=activityPackages,
           mappingType=collection,
           propertyType=List<ActivityPackageRow>,
           sourceTable=pkg,
           collectionKey=package__doc_id,
           aliasPrefix=package__
           notNull=true } */

FROM activity_bundle_detailed_view bndl
    LEFT JOIN activity_package_to_join pkg ON bndl.bundle__doc_id = pkg.package__activity_bundle_doc_id;


-- Activity bundle with detailed packages
-- @@{ collectionKey=bundle__doc_id }
CREATE VIEW activity_bundle_with_activities_view AS
SELECT
    bndl.*,
    pkg.*

    /* @@{ dynamicField=main,
           mappingType=entity,
           propertyType=ActivityBundleDetailedRow,
           sourceTable=bndl,
           aliasPrefix=bundle__
           notNull=true } */

    /* @@{ dynamicField=activityPackages,
           mappingType=collection,
           propertyType=List<ActivityPackageWithActivitiesRow>,
           sourceTable=pkg,
           collectionKey=package__doc_id,
           aliasPrefix=package__
           notNull=true } */

FROM activity_bundle_detailed_view bndl
    LEFT JOIN activity_package_with_activities_view pkg ON bndl.bundle__doc_id = pkg.package__activity_bundle_doc_id;
