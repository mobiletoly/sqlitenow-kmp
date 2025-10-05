/* @@{ queryResult=ActivityPackageWithActivitiesRow,
       mapTo=com.pluralfusion.daytempo.domain.model.ActivityPackageWithActivitiesDoc } */
SELECT pkg.*
FROM activity_package_with_activities_view pkg WHERE pkg.package__doc_id = :docId;
