/* @@{ sharedResult=CategoryWithPersonRow } */
SELECT
    c.id,
    c.name,
    c.description,
    c.created_at,

    p.id AS person_id,
    p.first_name AS person_myFirstName,
    p.last_name AS person_myLastName,
    p.email AS person_email,
    p.phone AS person_phone,
    p.birth_date AS person_birthDate,
    p.created_at AS person_createdAt

/* @@{ dynamicField=primaryPerson,
       mappingType=perRow,
       propertyType=PersonQuery.SharedResult.Row,
       sourceTable=p,
       aliasPrefix=person_,
       notNull=false } */

FROM category c
    LEFT JOIN person_category pc ON c.id = pc.category_id
    LEFT JOIN person p ON pc.person_id = p.id
WHERE c.id = :categoryId
