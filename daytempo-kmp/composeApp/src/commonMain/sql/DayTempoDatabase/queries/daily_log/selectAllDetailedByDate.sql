-- @@{ queryResult=DailyLogDetailedDoc }
SELECT
    dl.*
FROM daily_log_detailed_view dl
WHERE dl.dl__year = :year AND dl.dl__month = :month AND dl.dl__day = :day;
