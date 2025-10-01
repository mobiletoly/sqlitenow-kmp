-- @@{ queryResult=DailyLogDetailedDoc }
SELECT
    dl.*
FROM daily_log_detailed_view dl
WHERE dl.dl__date = :date;
