/* @@{ queryResult=CategoryWithPersonRow } */
SELECT
    c.id,
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

/* @@{ dynamicField=primaryPerson,
       mappingType=perRow,
       propertyType=PersonRow,
       sourceTable=p,
       aliasPrefix=person__,
       notNull=false } */

FROM category c
    LEFT JOIN person_category pc ON c.id = pc.category_id
    LEFT JOIN person p ON pc.person_id = p.id
WHERE c.id = :categoryId
