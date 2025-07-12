
-- Insert Person records
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (1, 'John', 'Smith', 'john.smith@example.com', '+1-555-123-4567', '1985-03-15');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (2, 'Emma', 'Johnson', 'emma.johnson@example.com', '+1-555-234-5678', '1990-07-22');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (3, 'Michael', 'Williams', 'michael.williams@example.com', '+1-555-345-6789', '1978-11-30');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (4, 'Sophia', 'Brown', 'sophia.brown@example.com', '+1-555-456-7890', '1992-05-18');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (5, 'James', 'Jones', 'james.jones@example.com', '+1-555-567-8901', '1983-09-04');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (6, 'Olivia', 'Garcia', 'olivia.garcia@example.com', '+1-555-678-9012', '1995-01-12');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (7, 'Robert', 'Miller', 'robert.miller@example.com', '+1-555-789-0123', '1975-06-28');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (8, 'Ava', 'Davis', 'ava.davis@example.com', '+1-555-890-1234', '1988-12-07');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (9, 'William', 'Rodriguez', 'william.rodriguez@example.com', '+1-555-901-2345', '1980-04-19');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (10, 'Isabella', 'Martinez', 'isabella.martinez@example.com', '+1-555-012-3456', '1993-08-25');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (11, 'John', 'Hernandez', 'david.hernandez@example.com', '+1-555-123-4567', '1982-02-14');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (12, 'Mia', 'Lopez', 'mia.lopez@example.com', '+1-555-234-5678', '1997-10-31');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (13, 'Joseph', 'Gonzalez', 'joseph.gonzalez@example.com', '+1-555-345-6789', '1976-07-09');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (14, 'Charlotte', 'Wilson', 'charlotte.wilson@example.com', '+1-555-456-7890', '1991-03-27');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (15, 'Thomas', 'Anderson', 'thomas.anderson@example.com', '+1-555-567-8901', '1984-11-05');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (16, 'Amelia', 'Taylor', 'amelia.taylor@example.com', '+1-555-678-9012', '1994-09-16');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (17, 'Charles', 'Moore', 'charles.moore@example.com', '+1-555-789-0123', '1979-05-23');
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) VALUES (18, 'Harper', 'Jackson', 'harper.jackson@example.com', '+1-555-890-1234', '1996-01-08');

-- Insert PersonAddress records
-- Home addresses
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (1, 1, 'home', '123 Main St', 'New York', 'NY', '10001', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (2, 2, 'home', '456 Oak Ave', 'Los Angeles', 'CA', '90001', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (3, 3, 'home', '789 Pine Rd', 'Chicago', 'IL', '60601', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (4, 4, 'home', '101 Maple Dr', 'Houston', 'TX', '77001', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (5, 5, 'home', '202 Cedar Ln', 'Phoenix', 'AZ', '85001', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (6, 6, 'home', '303 Birch Blvd', 'Philadelphia', 'PA', '19101', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (7, 7, 'home', '404 Elm St', 'San Antonio', 'TX', '78201', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (8, 8, 'home', '505 Willow Way', 'San Diego', 'CA', '92101', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (9, 9, 'home', '606 Spruce St', 'Dallas', 'TX', '75201', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (10, 10, 'home', '707 Aspen Ave', 'San Jose', 'CA', '95101', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (11, 11, 'home', '808 Redwood Rd', 'Austin', 'TX', '73301', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (12, 12, 'home', '--- 909 Sequoia St', 'Jacksonville', 'FL', '32099', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (13, 13, 'home', '1010 Magnolia Dr', 'Fort Worth', 'TX', '76101', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (14, 14, 'home', '1111 Dogwood Dr', 'Columbus', 'OH', '43085', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (15, 15, 'home', '1212 Sycamore St', 'Charlotte', 'NC', '28201', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (16, 16, 'home', '1313 Poplar Pl', 'Seattle', 'WA', '98101', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (17, 17, 'home', '1414 Juniper Ln', 'Denver', 'CO', '80201', 'USA', 1);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (18, 18, 'home', '1515 Hawthorn Rd', 'Boston', 'MA', '02101', 'USA', 1);

-- Work addresses for some people
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (19, 1, 'work', '100 Business Plaza', 'New York', 'NY', '10002', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (20, 3, 'work', '200 Corporate Center', 'Chicago', 'IL', '60602', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (21, 5, 'work', '300 Office Park', 'Phoenix', 'AZ', '85002', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (22, 7, 'work', '400 Tech Campus', 'San Antonio', 'TX', '78202', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (23, 9, 'work', '500 Innovation Hub', 'Dallas', 'TX', '75202', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (24, 11, 'work', '600 Research Park', 'Austin', 'TX', '73302', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (25, 13, 'work', '700 Industrial Blvd', 'Fort Worth', 'TX', '76102', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (26, 15, 'work', '800 Commerce St', 'Charlotte', 'NC', '28202', 'USA', 0);
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (27, 17, 'work', '900 Enterprise Way', 'Denver', 'CO', '80202', 'USA', 0);


-- Add comments
INSERT INTO Comment (person_id, comment, created_at, tags)
VALUES (1, 'Hello World #1', '2021-01-01 12:00:00', '["hello", "world"]');
INSERT INTO Comment (person_id, comment, created_at, tags)
VALUES (1, 'Hello World #2', '2021-01-01 12:00:00', '["hello", "world"]');
INSERT INTO Comment (person_id, comment, created_at, tags)
VALUES (2, 'This is a comment.', '2021-01-02 12:00:00', '["comment"]');
INSERT INTO Comment (person_id, comment, created_at, tags)
VALUES (3, 'Another comment.', '2021-01-03 12:00:00', '["comment"]');
