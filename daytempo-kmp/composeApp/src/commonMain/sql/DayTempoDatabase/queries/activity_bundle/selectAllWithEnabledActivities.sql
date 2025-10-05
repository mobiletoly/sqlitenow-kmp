/* @@{ queryResult=ActivityBundleWithActivitiesRow,
       mapTo=com.pluralfusion.daytempo.domain.model.ActivityBundleWithActivitiesDoc } */
SELECT * FROM activity_bundle_with_activities_view
WHERE bundle__installed_at IS NOT NULL
AND act__enabled = 1
ORDER BY bundle__title, package__title;
