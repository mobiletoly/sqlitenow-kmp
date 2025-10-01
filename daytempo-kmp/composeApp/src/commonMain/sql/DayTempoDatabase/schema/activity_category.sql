CREATE TABLE activity_category (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    title TEXT NOT NULL DEFAULT '',

    -- @@{ field=icon, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    icon TEXT NOT NULL
) WITHOUT ROWID;
