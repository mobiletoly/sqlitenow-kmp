-- @@{ queryResult=DailyLogDetailedRow }
WITH range AS (
  SELECT
    (julianday(printf('%04d-%02d-01', CAST(:year AS INTEGER), CAST(:month AS INTEGER))) - julianday('1970-01-01')) AS start_days,
    (julianday(date(printf('%04d-%02d-01', :year, :month), '+1 month')) - julianday('1970-01-01')) AS end_days
)
SELECT *
FROM daily_log_detailed_view
WHERE dl__date >= (SELECT start_days FROM range)
  AND dl__date <  (SELECT end_days FROM range);
