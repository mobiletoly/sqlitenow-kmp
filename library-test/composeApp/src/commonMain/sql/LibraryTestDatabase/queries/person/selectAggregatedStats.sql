-- @@{ queryResult=PersonAggregateResult, mapTo=dev.goquick.sqlitenow.librarytest.db.PersonAggregateSummary }
SELECT
    COUNT(*) AS total_count,
    AVG(LENGTH(first_name)) AS avg_first_name_length
FROM person;
