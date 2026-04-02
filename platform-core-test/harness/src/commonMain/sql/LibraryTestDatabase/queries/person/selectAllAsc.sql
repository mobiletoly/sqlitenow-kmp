-- @@{ queryResult=PersonSummaryResult, mapTo=dev.goquick.sqlitenow.core.test.db.PersonSummary }
SELECT
    id,
    first_name,
    last_name
FROM person
ORDER BY last_name ASC, first_name ASC;
