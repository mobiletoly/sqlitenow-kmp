-- @@{ queryResult=ActivityBundleDetailedDoc }
SELECT bndl.* FROM activity_bundle_detailed_view bndl WHERE bndl.bundle__doc_id = :docId;
