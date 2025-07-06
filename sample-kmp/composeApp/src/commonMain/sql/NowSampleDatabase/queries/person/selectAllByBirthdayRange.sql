SELECT *
FROM Person
WHERE birth_date >= :startDate
  AND birth_date <= :endDate;
