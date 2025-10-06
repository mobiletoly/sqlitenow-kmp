/* @@{ queryResult=CategoryWithPersonsRow, collectionKey=category_id } */
SELECT
    c.id AS category_id,
    c.name,
    c.description,
    c.created_at,

    p.id AS person__id,
    p.first_name AS person__myFirstName,
    p.last_name AS person__myLastName,
    p.email AS person__email,
    p.phone AS person__phone,
    p.birth_date AS person__birthDate,
    p.created_at AS person__createdAt

/* @@{ dynamicField=persons,
       mappingType=collection,
       propertyType=List<PersonRow>,
       sourceTable=p,
       collectionKey=person__id,
       aliasPrefix=person__,
       notNull=true } */

FROM category c
    LEFT JOIN person_category pc ON c.id = pc.category_id
    LEFT JOIN person p ON pc.person_id = p.id
WHERE c.id = :categoryId
