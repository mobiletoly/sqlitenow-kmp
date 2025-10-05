UPDATE activity
SET enabled = :enabled,
    activity_package_doc_id = :activityPackageDocId,
    category_doc_id = :categoryDocId,
    title = :title,
    descr = :descr,
    program_type = :programType,
    icon = :icon,
    monthly_glance_view = :monthlyGlanceView,
    priority = :priority
WHERE doc_id = :docId;
