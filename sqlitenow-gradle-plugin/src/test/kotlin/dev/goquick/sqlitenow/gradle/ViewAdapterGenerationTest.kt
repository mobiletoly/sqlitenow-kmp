package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import java.io.File
import kotlin.test.assertTrue

class ViewAdapterGenerationTest {
    @Test
    fun adapters_are_generated_and_used_for_view_projected_columns() {
        val root = createTempDir(prefix = "view-adapter-")
        val schemaDir = File(root, "schema").apply { mkdirs() }
        val queriesDir = File(root, "queries").apply { mkdirs() }

        // Activity table
        File(schemaDir, "activity.sql").writeText(
            """
            CREATE TABLE activity (
              id BLOB PRIMARY KEY NOT NULL,
              doc_id TEXT NOT NULL UNIQUE,
              category_doc_id TEXT NOT NULL
            ) WITHOUT ROWID;
            """.trimIndent()
        )

        // Category with custom type for icon + view projection
        File(schemaDir, "activity_category.sql").writeText(
            """
            CREATE TABLE activity_category (
              id BLOB PRIMARY KEY NOT NULL,
              doc_id TEXT NOT NULL UNIQUE,
              title TEXT NOT NULL,
              -- @@{ field=icon, propertyType=com.example.IconDoc }
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

        // Provide shared result for category (so type is available by name)
        val catDir = File(queriesDir, "activity_category").apply { mkdirs() }
        File(catDir, "selectAll.sql").writeText(
            """
            -- @@{ sharedResult=Row }
            SELECT doc_id, title, icon FROM activity_category;
            """.trimIndent()
        )

        // Query selecting from view and enabling joined result generation via a dynamic field
        val actDir = File(queriesDir, "activity").apply { mkdirs() }
        File(actDir, "loadFromView.sql").writeText(
            """
            /* @@{ sharedResult=RowWithCategory } */
            SELECT
              act.*,
              cat.*

            /* @@{ dynamicField=categoryDoc,
                   mappingType=perRow,
                   propertyType=ActivityCategoryQuery.SharedResult.Row,
                   sourceTable=cat,
                   aliasPrefix=joined_category_ } */

            FROM activity act
            LEFT JOIN activity_category_join_view cat ON act.category_doc_id = cat.joined_category_doc_id
            WHERE act.doc_id = :docId;
            """.trimIndent()
        )

        val outDir = File(root, "out").apply { mkdirs() }
        generateDatabaseFiles(
            dbName = "TestDbAdapters",
            sqlDir = root,
            packageName = "dev.test.adapters",
            outDir = outDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        val gen = outDir.walkTopDown().first { it.name.contains("ActivityQuery_LoadFromView") && it.extension == "kt" }
        val code = gen.readText()

        // Verify signature includes adapter for icon based on custom propertyType
        assertTrue(
            code.contains("sqlValueToIcon: (String) -> IconDoc"),
            "readJoinedStatementResult should request sqlValueToIcon adapter with correct types"
        )
        // Verify joined field uses the adapter (simple contains)
        assertTrue(code.contains("joinedCategoryIcon = if (statement.isNull("))
        assertTrue(code.contains("else sqlValueToIcon(statement.getText("))
    }
}
