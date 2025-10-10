-- @@{ queryResult=ActivityDetailedRow, mapTo=com.pluralfusion.daytempo.domain.model.ActivityDoc }
SELECT act.* FROM activity_detailed_view act
WHERE act__sched_read_ref_start_at = :scheduleReadRefStartAt;
