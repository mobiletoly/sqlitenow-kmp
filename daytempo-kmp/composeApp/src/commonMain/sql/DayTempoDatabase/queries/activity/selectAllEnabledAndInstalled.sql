-- @@{ queryResult=ActivityDetailedRow }
SELECT act.* FROM activity_detailed_view act
JOIN activity_bundle_to_join AS bndl ON act.act__activity_bundle_doc_id = bndl.bundle__doc_id
WHERE act__enabled = 1 AND act__deleted = 0 AND bndl.bundle__installed_at IS NOT NULL ;
