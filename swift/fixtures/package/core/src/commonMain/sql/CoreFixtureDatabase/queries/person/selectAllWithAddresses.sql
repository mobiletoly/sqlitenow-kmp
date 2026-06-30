/* @@{
    queryResult=PersonWithAddressRow,
    collectionKey=person__id } */
SELECT p.id AS person__id,
       p.first_name AS person__first_name,

       a.id AS address__id,
       a.person_id AS address__person_id,
       a.street AS address__street,
       a.is_primary AS address__is_primary,

       c.id AS comment__id,
       c.person_id AS comment__person_id,
       c.body AS comment__body

/* @@{ dynamicField=addresses,
       mappingType=collection,
       propertyType=List<PersonAddressRow>,
       sourceTable=a,
       collectionKey=address__id,
       aliasPrefix=address__,
       notNull=true } */

/* @@{ dynamicField=comments,
       mappingType=collection,
       propertyType=List<CommentRow>,
       sourceTable=c,
       collectionKey=comment__id,
       aliasPrefix=comment__,
       notNull=true } */

FROM person p
         LEFT JOIN person_address a ON p.id = a.person_id
         LEFT JOIN comment c ON p.id = c.person_id
ORDER BY p.id, a.id, c.id;
