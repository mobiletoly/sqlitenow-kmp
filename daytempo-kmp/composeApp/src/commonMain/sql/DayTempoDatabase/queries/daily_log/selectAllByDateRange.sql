-- @@{ queryResult=DailyLogDetailedRow }
SELECT
    dl.*
FROM daily_log_detailed_view dl
WHERE dl.dl__date BETWEEN :start AND :end;
