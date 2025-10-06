/* @@{ queryResult=PersonWithNestedCollectionsRow, collectionKey=person_id } */
SELECT
    p.id AS person_id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at,
    
    -- Address collection
    a.id AS address__id,
    a.person_id AS address__person_id,
    a.address_type AS address__address_type,
    a.street AS address__street,
    a.city AS address__city,
    a.state AS address__state,
    a.postal_code AS address__postal_code,
    a.country AS address__country,
    a.is_primary AS address__is_primary,
    a.created_at AS address__created_at,
    
    -- Comment collection
    c.id AS comment__id,
    c.person_id AS comment__person_id,
    c.comment AS comment__comment,
    c.created_at AS comment__created_at,
    c.tags AS comment__tags,
    
    -- Category collection (through junction table)
    cat.id AS category__id,
    cat.name AS category__name,
    cat.description AS category__description,
    cat.created_at AS category__created_at

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

  /* @@{ dynamicField=categories,
         mappingType=collection,
         propertyType=List<CategoryRow>,
         sourceTable=cat,
         collectionKey=category__id,
         aliasPrefix=category__,
         notNull=true } */

FROM person p
    LEFT JOIN person_address a ON p.id = a.person_id
    LEFT JOIN comment c ON p.id = c.person_id
    LEFT JOIN person_category pc ON p.id = pc.person_id
    LEFT JOIN category cat ON pc.category_id = cat.id
WHERE p.id = :personId
