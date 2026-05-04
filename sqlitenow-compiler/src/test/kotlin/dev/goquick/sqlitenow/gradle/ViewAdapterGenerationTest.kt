package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import java.io.File
import kotlin.io.path.createTempDirectory
import kotlin.test.assertTrue

class ViewAdapterGenerationTest {
    @Test
    fun adapters_are_generated_and_used_for_view_projected_columns() {
        val root = createTempDirectory(prefix = "view-adapter-").toFile()
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

            CREATE VIEW activity_category_for_join AS
            SELECT
              cat.doc_id AS category__doc_id,
              cat.title AS category__title,
              cat.icon AS category__icon
            FROM activity_category AS cat;
            """.trimIndent()
        )

        // Provide shared result for category (so type is available by name)
        val catDir = File(queriesDir, "activity_category").apply { mkdirs() }
        File(catDir, "selectAll.sql").writeText(
            """
            -- @@{ queryResult=Row }
            SELECT doc_id, title, icon FROM activity_category;
            """.trimIndent()
        )

        // Query selecting from view and enabling joined result generation via a dynamic field
        val actDir = File(queriesDir, "activity").apply { mkdirs() }
        File(actDir, "loadFromView.sql").writeText(
            """
            /* @@{ queryResult=RowWithCategory } */
            SELECT
              act.*,
              cat.*

            /* @@{ dynamicField=categoryDoc,
                   mappingType=perRow,
                   propertyType=ActivityCategoryQuery.SharedResult.Row,
                   sourceTable=cat,
                   aliasPrefix=category__ } */

            FROM activity act
            LEFT JOIN activity_category_for_join cat ON act.category_doc_id = cat.category__doc_id
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
        // Verify joined field uses the adapter - check for various possible patterns
        assertTrue(
            code.contains("joinedCategoryIcon = if (statement.isNull(") ||
            code.contains("categoryIcon = if (statement.isNull(") ||
            code.contains("sqlValueToIcon(statement.getText("),
            "Generated code should contain adapter usage for icon field"
        )
        assertTrue(
            code.contains("else sqlValueToIcon(statement.getText(") ||
            code.contains("sqlValueToIcon(statement.getText("),
            "Generated code should contain sqlValueToIcon adapter call"
        )
    }
}
