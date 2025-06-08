-- @@sharedResult=Row
-- @@implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields
-- @@excludeOverrideFields=[phone, birthDate, createdAt, notes, totalPersonCount]
SELECT
    *
FROM Person
ORDER BY id DESC
LIMIT :limit OFFSET :offset
