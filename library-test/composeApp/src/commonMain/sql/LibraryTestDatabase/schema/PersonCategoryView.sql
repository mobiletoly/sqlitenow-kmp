-- CREATE VIEW for complex collection mapping with hierarchical data
CREATE VIEW person_category_view AS
SELECT 
    p.id AS person_id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.birth_date,
    p.created_at AS person_created_at,
    
    c.id AS category_id,
    c.name AS category_name,
    c.description AS category_description,
    c.created_at AS category_created_at,
    
    pc.assigned_at,
    pc.is_primary,
    
    pa.id AS address_id,
    pa.address_type,
    pa.street,
    pa.city,
    pa.state,
    pa.postal_code,
    pa.country,
    pa.is_primary AS address_is_primary,
    pa.created_at AS address_created_at

FROM person p
    LEFT JOIN person_category pc ON p.id = pc.person_id
    LEFT JOIN category c ON pc.category_id = c.id
    LEFT JOIN person_address pa ON p.id = pa.person_id;
