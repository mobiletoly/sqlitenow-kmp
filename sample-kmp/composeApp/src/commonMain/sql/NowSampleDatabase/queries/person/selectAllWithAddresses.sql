/* @@{
    sharedResult=PersonWithAddressRow
} */
SELECT p.id         AS person_id,
       p.first_name,
       p.last_name,
       p.email,
       p.phone,
       p.birth_date,
       p.created_at AS person_created_at,
       a.id         AS address_id,
       a.address_type,
       a.street,
       a.city,
       a.state,
       a.postal_code,
       a.country,
       a.is_primary,
       a.created_at AS address_created_at

-- @@{ dynamicField=addresses, propertyType=List<PersonAddressQuery.SharedResult.Row>, defaultValue=listOf() }

FROM Person p
         JOIN PersonAddress a ON p.id = a.person_id
