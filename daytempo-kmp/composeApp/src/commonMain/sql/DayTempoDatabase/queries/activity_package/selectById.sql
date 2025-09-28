-- @@{ queryResult=ActivityPackageDoc }
SELECT pkg.*
FROM activity_package pkg WHERE pkg.doc_id = :docId;
