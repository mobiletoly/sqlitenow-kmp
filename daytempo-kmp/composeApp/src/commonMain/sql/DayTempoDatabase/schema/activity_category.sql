CREATE TABLE activity_category (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    title TEXT NOT NULL DEFAULT '',

    -- @@{ field=icon, propertyType=com.pluralfusion.daytempo.domain.model.ActivityIconDoc }
    icon TEXT NOT NULL
) WITHOUT ROWID;

CREATE VIEW activity_category_to_join AS
SELECT
    cat.id AS category__id,
    cat.doc_id AS category__doc_id,
    cat.title AS category__title,
    cat.icon AS category__icon
FROM activity_category AS cat;
