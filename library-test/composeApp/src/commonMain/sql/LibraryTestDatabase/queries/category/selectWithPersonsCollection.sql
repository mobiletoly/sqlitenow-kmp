/* @@{ sharedResult=CategoryWithPersonsRow, collectionKey=category_id } */
SELECT
    c.id AS category_id,
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

/* @@{ dynamicField=persons,
       mappingType=collection,
       propertyType=List<PersonQuery.SharedResult.Row>,
       sourceTable=p,
       collectionKey=person_id,
       aliasPrefix=person_,
       notNull=true } */

FROM category c
    LEFT JOIN person_category pc ON c.id = pc.category_id
    LEFT JOIN person p ON pc.person_id = p.id
WHERE c.id = :categoryId
