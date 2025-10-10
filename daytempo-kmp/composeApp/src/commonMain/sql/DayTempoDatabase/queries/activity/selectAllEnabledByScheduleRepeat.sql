-- @@{ queryResult=ActivityDetailedRow, mapTo=com.pluralfusion.daytempo.domain.model.ActivityDoc }
SELECT act.* FROM activity_detailed_view act
JOIN activity_bundle_to_join AS bndl ON act__activity_bundle_doc_id = bndl.bundle__doc_id
WHERE act__enabled = 1 AND act__deleted = 0 AND act__sched_repeat = :scheduleRepeat
AND act__sched_time_points != '';
