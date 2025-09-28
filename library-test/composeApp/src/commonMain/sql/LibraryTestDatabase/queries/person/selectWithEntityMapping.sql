-- Test mappingType=entity - maps columns from main FROM table to nested objects (no JOINs required)
SELECT 
    id,
    first_name,
    last_name,
    email,
    phone,
    birth_date,
    created_at,
    
    -- Entity mapping: map person table columns to a nested PersonDetails object
    id AS person_details__id,
    first_name AS person_details__first_name,
    last_name AS person_details__last_name,
    email AS person_details__email,
    phone AS person_details__phone,
    birth_date AS person_details__birth_date,
    created_at AS person_details__created_at

/* @@{ dynamicField=personDetails,
       mappingType=entity,
       propertyType=PersonRow,
       sourceTable=p,
       aliasPrefix=person_details__,
       notNull=true } */

FROM person p
WHERE p.id = :personId
