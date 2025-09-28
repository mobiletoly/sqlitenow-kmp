-- @@{ queryResult=ActivityPackageDetailedDoc }
SELECT pkg.*
FROM activity_package_detailed_view pkg WHERE pkg.package__doc_id = :docId;
