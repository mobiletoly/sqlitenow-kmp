/* @@{ queryResult=ActivityBundleFullRow,
       mapTo=com.pluralfusion.daytempo.domain.model.ActivityBundleFullDoc } */
SELECT * FROM activity_bundle_full_view
WHERE bundle__installed_at IS NOT NULL
AND act__enabled = 1
ORDER BY bundle__title, package__title;
