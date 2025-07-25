-- @@{ sharedResult=PersonWithLimitedAddressRow }
SELECT p.id         AS person_id,
       p.first_name,
       p.last_name,
       p.email,
       p.phone,
       p.birth_date,
       p.created_at,

       a.id         AS address_id,
       a.person_id  AS address_person_id,
       a.address_type,
       a.postal_code,
       a.country,
       a.is_primary,
       a.created_at AS address_created_at,
       a.street,
       a.city,
       a.state,

       c.id         AS comment_id,
       c.person_id  AS comment_person_id,
       c.comment    AS comment_comment,
       c.created_at AS comment_created_at,
       c.tags

/* @@{ dynamicField=address,
       mappingType=perRow,
       propertyType=PersonAddressQuery.SharedResult.Row,
       sourceTable=a,
       removeAliasPrefix=address_ } */

/* @@{ dynamicField=comment,
       mappingType=perRow,
       propertyType=CommentQuery.SharedResult.Row,
       sourceTable=c,
       removeAliasPrefix=comment_ } */

FROM Person p
         LEFT JOIN PersonAddress a ON p.id = a.person_id
         LEFT JOIN Comment c ON p.id = c.person_id
ORDER BY p.id, a.address_type
LIMIT 100
