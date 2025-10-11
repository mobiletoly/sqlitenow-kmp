-- @@{ queryResult=DailyLogDetailedRow, mapTo=com.pluralfusion.daytempo.domain.model.DailyLogDoc }
SELECT
    dl.*
FROM daily_log_detailed_view dl
WHERE dl.dl__activity_doc_id = :activityDocId;
