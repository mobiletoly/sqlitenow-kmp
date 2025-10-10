-- @@{ queryResult=ActivityDetailedRow, mapTo=com.pluralfusion.daytempo.domain.model.ActivityDoc }
SELECT act.* FROM activity_detailed_view act
WHERE act__sched_write_ref_days_duration = :scheduleWriteRefDaysDuration
LIMIT 1;
