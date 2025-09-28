SELECT bndl.*, prov.title AS provider_title FROM activity_bundle bndl
JOIN provider AS prov ON bndl.provider_doc_id = prov.doc_id
WHERE bndl.doc_id = :docId;
