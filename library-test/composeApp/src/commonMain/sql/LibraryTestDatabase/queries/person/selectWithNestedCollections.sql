/* @@{ sharedResult=PersonWithNestedCollectionsRow, collectionKey=person_id } */
SELECT
    p.id AS person_id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at,
    
    -- Address collection
    a.id AS address_id,
    a.person_id AS address_person_id,
    a.address_type,
    a.street,
    a.city,
    a.state,
    a.postal_code,
    a.country,
    a.is_primary AS address_is_primary,
    a.created_at AS address_created_at,
    
    -- Comment collection
    c.id AS comment_id,
    c.person_id AS comment_person_id,
    c.comment,
    c.created_at AS comment_created_at,
    c.tags,
    
    -- Category collection (through junction table)
    cat.id AS category_id,
    cat.name AS category_name,
    cat.description AS category_description,
    cat.created_at AS category_created_at

  /* @@{ dynamicField=addresses,
         mappingType=collection,
         propertyType=List<PersonAddressQuery.SharedResult.Row>,
         sourceTable=a,
         collectionKey=address_id,
         aliasPrefix=address_,
         notNull=true } */

  /* @@{ dynamicField=comments,
         mappingType=collection,
         propertyType=List<CommentQuery.SharedResult.Row>,
         sourceTable=c,
         collectionKey=comment_id,
         aliasPrefix=comment_,
         notNull=true } */

  /* @@{ dynamicField=categories,
         mappingType=collection,
         propertyType=List<CategoryQuery.SharedResult.Row>,
         sourceTable=cat,
         collectionKey=category_id,
         aliasPrefix=category_,
         notNull=true } */

FROM person p
    LEFT JOIN person_address a ON p.id = a.person_id
    LEFT JOIN comment c ON p.id = c.person_id
    LEFT JOIN person_category pc ON p.id = pc.person_id
    LEFT JOIN category cat ON pc.category_id = cat.id
WHERE p.id = :personId
