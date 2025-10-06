CREATE TABLE parent_entity (
    id INTEGER PRIMARY KEY NOT NULL,
    doc_id TEXT NOT NULL UNIQUE,
    category_id INTEGER NOT NULL
);

CREATE TABLE parent_category (
    id INTEGER PRIMARY KEY NOT NULL,
    doc_id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL
);

CREATE TABLE child_entity (
    id INTEGER PRIMARY KEY NOT NULL,
    parent_doc_id TEXT NOT NULL,
    title TEXT NOT NULL
);

CREATE TABLE child_schedule (
    id INTEGER PRIMARY KEY NOT NULL,
    child_id INTEGER NOT NULL,
    frequency TEXT,
    start_day INTEGER
);

-- Base views with prefixed columns
CREATE VIEW parent_to_join AS
SELECT
    p.id AS parent__id,
    p.doc_id AS parent__doc_id,
    p.category_id AS parent__category_id
FROM parent_entity AS p;

CREATE VIEW parent_category_to_join AS
SELECT
    c.id AS category__id,
    c.doc_id AS category__doc_id,
    c.title AS category__title
FROM parent_category AS c;

CREATE VIEW child_to_join AS
SELECT
    ch.id AS child__id,
    ch.parent_doc_id AS child__parent_doc_id,
    ch.title AS child__title
FROM child_entity AS ch;

CREATE VIEW child_schedule_to_join AS
SELECT
    s.id AS schedule__id,
    s.child_id AS schedule__child_id,
    s.frequency AS schedule__frequency,
    s.start_day AS schedule__start_day
FROM child_schedule AS s;

-- Parent detailed view with entity + perRow mapping
CREATE VIEW parent_detailed_view AS
SELECT
    parent.*,
    category.*

    /* @@{ dynamicField=main,
           mappingType=entity,
           propertyType=ParentMainDoc,
           sourceTable=parent,
           aliasPrefix=parent__,
           notNull=true } */

    /* @@{ dynamicField=category,
           mappingType=perRow,
           propertyType=ParentCategoryDoc,
           sourceTable=category,
           aliasPrefix=category__,
           notNull=true } */

FROM parent_to_join parent
LEFT JOIN parent_category_to_join category ON parent.parent__category_id = category.category__id;

-- Child detailed view with entity + schedule mapping
CREATE VIEW child_detailed_view AS
SELECT
    child.*,
    sched.*

    /* @@{ dynamicField=main,
           mappingType=entity,
           propertyType=ChildMainDoc,
           sourceTable=child,
           aliasPrefix=child__,
           notNull=true } */

    /* @@{ dynamicField=schedule,
           mappingType=perRow,
           propertyType=ChildScheduleDoc,
           sourceTable=sched,
           aliasPrefix=schedule__ } */

FROM child_to_join child
LEFT JOIN child_schedule_to_join sched ON child.child__id = sched.schedule__child_id;

-- Parent view with children collection
-- @@{ collectionKey=parent__doc_id }
CREATE VIEW parent_with_children_view AS
SELECT
    pdv.*,
    cdv.*

    /* @@{ dynamicField=children,
           mappingType=collection,
           propertyType=List<ParentChildDoc>,
           sourceTable=cdv,
           collectionKey=child__id,
           aliasPrefix=child__,
           notNull=true } */

FROM parent_detailed_view pdv
LEFT JOIN child_detailed_view cdv ON pdv.parent__doc_id = cdv.child__parent_doc_id;
