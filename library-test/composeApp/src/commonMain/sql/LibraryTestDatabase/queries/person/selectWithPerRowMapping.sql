-- Test mappingType=perRow - maps data from JOIN tables to nested objects
SELECT 
    p.id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at,
    
    -- perRow mapping: map address table columns to a nested Address object
    a.id AS address__id,
    a.person_id AS address__person_id,
    a.address_type AS address__address_type,
    a.street AS address__street,
    a.city AS address__city,
    a.state AS address__state,
    a.postal_code AS address__postal_code,
    a.country AS address__country,
    a.is_primary AS address__is_primary,
    a.created_at AS address__created_at

/* @@{ dynamicField=address,
       mappingType=perRow,
       propertyType=PersonAddressRow,
       sourceTable=a,
       aliasPrefix=address__ } */

FROM person p
LEFT JOIN person_address a ON p.id = a.person_id
WHERE p.id = :personId
