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
