-- @@sharedResult=Row
-- @@implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields
-- @@excludeOverrideFields=[phone, birthDate, createdAt, notes, totalPersonCount]
SELECT *
FROM Person
WHERE birth_date >= :startDate
  AND birth_date <= :endDate
