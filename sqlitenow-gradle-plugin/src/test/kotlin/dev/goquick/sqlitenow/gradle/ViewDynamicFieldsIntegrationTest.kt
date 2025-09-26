package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import java.io.File
import kotlin.test.assertTrue

class ViewDynamicFieldsIntegrationTest {
    @Test
    fun generates_code_for_select_from_view_with_dynamic_fields() {
        // Arrange: create a temporary SQL directory with schema + queries
        val root = createTempDir(prefix = "view-dyn-int-")
        val schemaDir = File(root, "schema").apply { mkdirs() }
        val queriesDir = File(root, "queries").apply { mkdirs() }

        // Minimal tables
        File(schemaDir, "activity.sql").writeText(
            """
            CREATE TABLE activity (
                id BLOB PRIMARY KEY NOT NULL,
                doc_id TEXT NOT NULL UNIQUE,
                category_doc_id TEXT NOT NULL
            ) WITHOUT ROWID;
            """.trimIndent()
        )
        File(schemaDir, "activity_category.sql").writeText(
            """
            CREATE TABLE activity_category (
                id BLOB PRIMARY KEY NOT NULL,
                doc_id TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                icon TEXT NOT NULL
            ) WITHOUT ROWID;

            CREATE VIEW activity_category_join_view AS
            SELECT
                cat.doc_id AS joined_category_doc_id,
                cat.title AS joined_category_title,
                cat.icon AS joined_category_icon
            FROM activity_category AS cat;
            """.trimIndent()
        )

        // Queries: shared result for activity_category
        val qCatDir = File(queriesDir, "activity_category").apply { mkdirs() }
        File(qCatDir, "selectById.sql").writeText(
            """
            -- @@{ sharedResult=Row }
            SELECT doc_id, title, icon FROM activity_category WHERE doc_id = :docId;
            """.trimIndent()
        )

        // Query: select from view using act.* and cat.* with dynamic field mapped perRow
        val qActDir = File(queriesDir, "activity").apply { mkdirs() }
        File(qActDir, "loadByDocId.sql").writeText(
            """
            SELECT
                act.*,

                cat.*

            /* @@{ dynamicField=categoryDoc,
                   mappingType=perRow,
                   propertyType=ActivityCategoryQuery.SharedResult.Row,
                   sourceTable=cat,
                   aliasPrefix=joined_category_,
                   notNull=true } */

            FROM activity act
            LEFT JOIN activity_category_join_view cat ON act.category_doc_id = cat.joined_category_doc_id
            WHERE act.doc_id = :docId;
            """.trimIndent()
        )

        val outDir = File(root, "out").apply { mkdirs() }

        // Act: run generation
        generateDatabaseFiles(
            dbName = "TestDb",
            sqlDir = root,
            packageName = "dev.test",
            outDir = outDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        // Assert: some expected files exist (ActivityQuery_LoadByDocId should be generated)
        val generated = outDir.walkTopDown().filter { it.isFile && it.extension == "kt" }.toList()
        assertTrue(generated.any { it.name.contains("ActivityQuery_LoadByDocId") }, "Expected ActivityQuery_LoadByDocId to be generated")
    }
}

