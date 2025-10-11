-- @@{ queryResult=DailyLogDetailedRow, mapTo=com.pluralfusion.daytempo.domain.model.DailyLogDoc }
SELECT *
FROM daily_log_detailed_view dl
WHERE dl.dl__activity_doc_id = :activityDocId
    AND dl.dl__date < :date
ORDER BY dl.dl__date DESC
LIMIT 1;
