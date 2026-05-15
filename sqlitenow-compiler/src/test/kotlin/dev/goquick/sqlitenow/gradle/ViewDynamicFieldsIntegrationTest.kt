package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

class ViewDynamicFieldsIntegrationTest {
    @Test
    fun generates_code_for_select_from_view_with_dynamic_fields() {
        // Arrange: create a temporary SQL directory with schema + queries
        val fixture = CodegenFixture.create(prefix = "view-dyn-int-")

        // Minimal tables
        fixture.writeSchema(
            "activity.sql",
            """
            CREATE TABLE activity (
                id BLOB PRIMARY KEY NOT NULL,
                doc_id TEXT NOT NULL UNIQUE,
                category_doc_id TEXT NOT NULL
            ) WITHOUT ROWID;
            """.trimIndent()
        )
        fixture.writeSchema(
            "activity_category.sql",
            """
            CREATE TABLE activity_category (
                id BLOB PRIMARY KEY NOT NULL,
                doc_id TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                icon TEXT NOT NULL
            ) WITHOUT ROWID;

            CREATE VIEW activity_category_for_join AS
            SELECT
                cat.doc_id AS category__doc_id,
                cat.title AS category__title,
                cat.icon AS category__icon
            FROM activity_category AS cat;
            """.trimIndent()
        )

        // Queries: shared result for activity_category
        fixture.writeQuery(
            "activity_category",
            "selectById.sql",
            """
            -- @@{ queryResult=Row }
            SELECT doc_id, title, icon FROM activity_category WHERE doc_id = :docId;
            """.trimIndent()
        )

        // Query: select from view using act.* and cat.* with dynamic field mapped perRow
        fixture.writeQuery(
            "activity",
            "loadByDocId.sql",
            """
            SELECT
                act.*,

                cat.*

            /* @@{ dynamicField=categoryDoc,
                   mappingType=perRow,
                   propertyType=ActivityCategoryQuery.SharedResult.Row,
                   sourceTable=cat,
                   aliasPrefix=category__,
                   notNull=true } */

            FROM activity act
            LEFT JOIN activity_category_for_join cat ON act.category_doc_id = cat.category__doc_id
            WHERE act.doc_id = :docId;
            """.trimIndent()
        )

        // Act: run generation
        fixture.generate()

        // Assert: some expected files exist (ActivityQuery_LoadByDocId should be generated)
        val generated = fixture.generatedFiles()
        assertTrue(generated.any { it.name.contains("ActivityQuery_LoadByDocId") }, "Expected ActivityQuery_LoadByDocId to be generated")
    }
}
