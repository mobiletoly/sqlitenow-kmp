-- @@{ queryResult=ActivityPackageWithActivitiesRow }
SELECT pkg.*
FROM activity_package_with_activities_view pkg WHERE pkg.package__doc_id = :docId;
