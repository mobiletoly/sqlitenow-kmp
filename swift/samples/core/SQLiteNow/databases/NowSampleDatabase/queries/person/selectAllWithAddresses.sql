/* @@{
    queryResult=PersonWithAddressRow,
    collectionKey=person__id } */
SELECT p.id AS person__id,
       p.first_name AS person__first_name,
       p.last_name AS person__last_name,
       p.email AS person__email,
       p.phone AS person__phone,
       p.birth_date AS person__birth_date,
       p.created_at AS person__created_at,
       p.notes AS person__notes,

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

/* @@{ dynamicField=addresses,
       mappingType=collection,
       propertyType=List<PersonAddressRow>,
       sourceTable=a,
       collectionKey=address__id,
       aliasPrefix=address__,
       notNull=true } */

FROM person p
         LEFT JOIN person_address a ON p.id = a.person_id
ORDER BY p.id DESC, a.is_primary DESC, a.id
LIMIT :limit OFFSET :offset;
