---
layout: doc
title: Initialize Data
permalink: /documentation/initialize-data/
parent: Documentation
---

# Initialize Data

Learn how to populate your database with initial data during database creation using SQL files.

## Directory Structure

Initial data files are placed in the `init/` directory within your database structure:

```
src/commonMain/sql/SampleDatabase/
├── schema/          # CREATE TABLE statements
├── queries/         # SELECT, INSERT, UPDATE, DELETE queries
└── init/            # Initial data files
    └── init.sql
```

## Basic Data Initialization

**File: `init/init.sql`**

```sql
-- Insert Person records
INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) 
VALUES (1, 'John', 'Smith', 'john.smith@example.com', '+1-555-123-4567', '1985-03-15');

INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) 
VALUES (2, 'Emma', 'Johnson', 'emma.johnson@example.com', '+1-555-234-5678', '1990-07-22');

INSERT INTO Person (id, first_name, last_name, email, phone, birth_date) 
VALUES (3, 'Michael', 'Williams', 'michael.williams@example.com', '+1-555-345-6789', '1978-11-30');
```

This file contains standard SQL INSERT statements that will be executed when the database is first created.

## Multiple Tables

You can initialize data for multiple tables in the same file:

```sql
-- Insert Person records
INSERT INTO Person (id, first_name, last_name, email) 
VALUES (1, 'John', 'Smith', 'john.smith@example.com');

INSERT INTO Person (id, first_name, last_name, email) 
VALUES (2, 'Emma', 'Johnson', 'emma.johnson@example.com');

-- Insert PersonAddress records
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (1, 1, 'HOME', '123 Main St', 'New York', 'NY', '10001', 'USA', 1);

INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (2, 2, 'HOME', '456 Oak Ave', 'Los Angeles', 'CA', '90001', 'USA', 1);

-- Work addresses
INSERT INTO PersonAddress (id, person_id, address_type, street, city, state, postal_code, country, is_primary)
VALUES (3, 1, 'WORK', '100 Business Plaza', 'New York', 'NY', '10002', 'USA', 0);
```

As you can see, you can insert data into multiple related tables, maintaining referential integrity
by using the correct foreign key relationships.

## When Initialization Runs

The initialization scripts run automatically when **Database is created for the first time** (when
your app starts and the database file doesn't exist)

The init scripts are executed **after** the schema is created.

## Multiple Files

You can organize your initialization data into multiple files:

```
init/
├── init.sql           # Main initialization
├── users.sql          # User data
├── categories.sql     # Category data
└── products.sql       # Product data
```

All `.sql` files in the `init/` directory will be executed in alphabetical order.

## Next Steps

[Migration]({{ site.baseurl }}/documentation/migration/) - Learn about database schema migration
