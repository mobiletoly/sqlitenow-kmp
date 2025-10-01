CREATE TABLE provider (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    doc_id TEXT NOT NULL UNIQUE,

    title TEXT NOT NULL
) WITHOUT ROWID;

CREATE VIEW provider_to_join AS
SELECT
    prov.id AS provider__id,
    prov.doc_id AS provider__doc_id,
    prov.title AS provider__title
FROM provider AS prov;
