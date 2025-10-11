-- @@{ queryResult=DailyLogDetailedRow, mapTo=com.pluralfusion.daytempo.domain.model.DailyLogDoc }
SELECT
    dl.*
FROM daily_log_detailed_view dl
WHERE dl.dl__date BETWEEN :start AND :end
AND dl.dl__activity_doc_id IN :activityDocIds;
