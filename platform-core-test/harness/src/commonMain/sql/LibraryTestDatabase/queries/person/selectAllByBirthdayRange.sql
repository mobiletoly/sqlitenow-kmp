SELECT *
FROM person
WHERE birth_date >= :startDate
  AND birth_date <= :endDate;
