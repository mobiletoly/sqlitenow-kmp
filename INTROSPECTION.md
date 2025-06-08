Return tables and views with column names, types, and source information

```sql
SELECT  m.name                               AS table_name,
        tx.cid                               AS column_id,      -- ordinal
        tx.name                              AS column_name,
        tx.type                              AS data_type,
        tx."notnull"                         AS not_null,       -- 1 = NOT NULL
        tx.dflt_value                        AS default_value,
        tx.pk                                AS pk_pos,         -- 1‑based in PK
        tx."hidden"                          AS hidden_flag,    -- 0/1/2/3
    /* Is this column covered by any UNIQUE index? */
        EXISTS (
            SELECT 1
            FROM   pragma_index_list(m.name)  AS il            -- all indexes
                       JOIN   pragma_index_info(il.name) AS ii            -- key columns
                              ON ii.name = tx.name
            WHERE  il."unique" = 1
            LIMIT  1
        ) AS is_unique,
    /* If the column is a child key, list parent table/col pairs */
        (SELECT group_concat(fk."table" || '(' || fk."to" || ')')
         FROM   pragma_foreign_key_list(m.name) AS fk
         WHERE  fk."from" = tx.name)            AS fk_target
FROM    sqlite_schema                 AS m
            JOIN    pragma_table_xinfo(m.name)    AS tx                 -- every column
WHERE   m.type = 'view'
  AND   m.name NOT LIKE 'sqlite_%'                        -- hide internals
ORDER BY m.name, tx.cid;
```
