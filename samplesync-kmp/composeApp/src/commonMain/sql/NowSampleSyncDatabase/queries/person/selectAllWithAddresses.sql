/* @@{
    queryResult=PersonWithAddressRow,
    collectionKey=person_id } */
SELECT p.id         AS person_id,
       p.first_name,
       p.last_name,
       p.email,
       p.phone,
       p.birth_date,
       p.created_at,

       a.id         AS address__id,
       a.person_id  AS address__person_id,
       a.address_type AS address__address_type,
       a.postal_code AS address__postal_code,
       a.country AS address__country,
       a.is_primary AS address__is_primary,
       a.created_at AS address__created_at,
       a.street AS address__street,
       a.city AS address__city,
       a.state AS address__state,

       c.id         AS comment__id,
       c.person_id  AS comment__person_id,
       c.comment    AS comment__comment,
       c.created_at AS comment__created_at,
       c.tags AS comment__tags

/* @@{ dynamicField=addresses,
       mappingType=collection,
       propertyType=List<PersonAddressRow>,
       sourceTable=a,
       collectionKey=address__id,
       notNull=true,
       aliasPrefix=address__ } */

/* @@{ dynamicField=comments,
       mappingType=collection,
       propertyType=List<CommentRow>,
       collectionKey=comment__id,
       sourceTable=c,
       notNull=true,
       aliasPrefix=comment__ } */

FROM person p
         LEFT JOIN person_address a ON p.id = a.person_id
         LEFT JOIN comment c ON p.id = c.person_id
ORDER BY p.id, a.address_type
LIMIT :limit OFFSET :offset
