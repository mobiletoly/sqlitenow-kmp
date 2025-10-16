package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertTrue

/**
 * Test to verify that the syncKeyColumnName annotation is properly handled
 * in code generation and generates correct SyncTable objects.
 */
class SyncKeyColumnNameAnnotationTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    @DisplayName("Test syncKeyColumnName annotation generates correct SyncTable objects")
    fun testSyncKeyColumnNameAnnotation() {
        // Create test schema with syncKeyColumnName annotation
        val schemaDir = File(tempDir, "schema")
        schemaDir.mkdirs()

        // Create table with explicit syncKeyColumnName annotation
        File(schemaDir, "users.sql").writeText(
            """
            -- @@{enableSync=true, syncKeyColumnName=uuid}
            CREATE TABLE users (
                uuid TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            );
        """.trimIndent()
        )

        // Create table with enableSync but no explicit syncKeyColumnName (should auto-detect)
        File(schemaDir, "products.sql").writeText(
            """
            -- @@{enableSync=true}
            CREATE TABLE products (
                code TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                price REAL NOT NULL
            );
        """.trimIndent()
        )

        // Create table with traditional id primary key
        File(schemaDir, "orders.sql").writeText(
            """
            -- @@{enableSync=true}
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_uuid TEXT NOT NULL,
                product_code TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1
            );
        """.trimIndent()
        )

        // Create output directory
        val outputDir = File(tempDir, "generated")
        outputDir.mkdirs()

        // Generate database files
        generateDatabaseFiles(
            dbName = "TestDatabase",
            sqlDir = tempDir,
            packageName = "com.test.db",
            outDir = outputDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        // Check that the generated database file contains correct SyncTable objects
        val generatedFiles = outputDir.walkTopDown()
            .filter { it.isFile && it.extension == "kt" }
            .toList()

        val databaseFile = generatedFiles.find { it.name.contains("TestDatabase") }
        assertTrue(databaseFile != null, "Database file should be generated")

        val generatedContent = databaseFile.readText()
        println("Generated content:")
        println(generatedContent)

        // Verify that SyncTable objects are generated correctly
        assertTrue(
            generatedContent.contains("syncTables: List<SyncTable>"),
            "Should generate syncTables property with correct type"
        )

        // Verify explicit syncKeyColumnName annotation is used
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"users\", syncKeyColumnName = \"uuid\")"),
            "Should use explicit syncKeyColumnName from annotation"
        )

        // Verify auto-detected primary key is used
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"products\", syncKeyColumnName = \"code\")"),
            "Should auto-detect primary key column name"
        )

        // Verify default id primary key doesn't include syncKeyColumnName parameter
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"orders\")") ||
            generatedContent.contains("SyncTable(tableName = \"orders\", syncKeyColumnName = \"id\")"),
            "Should handle default id primary key correctly"
        )

        // Verify buildOversqliteConfig function is generated
        assertTrue(
            generatedContent.contains("fun buildOversqliteConfig"),
            "Should generate buildOversqliteConfig function"
        )

        // Verify the function uses syncTables parameter
        assertTrue(
            generatedContent.contains("OversqliteConfig(schema, syncTables"),
            "buildOversqliteConfig should use syncTables parameter"
        )
    }

    @Test
    @DisplayName("Test that tables without enableSync annotation are not included in syncTables")
    fun testNonSyncTablesExcluded() {
        // Create test schema with mixed sync and non-sync tables
        val schemaDir = File(tempDir, "schema")
        schemaDir.mkdirs()

        // Sync table
        File(schemaDir, "sync_table.sql").writeText(
            """
            -- @@{enableSync=true, syncKeyColumnName=custom_id}
            CREATE TABLE sync_table (
                custom_id TEXT PRIMARY KEY NOT NULL,
                data TEXT NOT NULL
            );
        """.trimIndent()
        )

        // Non-sync table
        File(schemaDir, "regular_table.sql").writeText(
            """
            CREATE TABLE regular_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info TEXT NOT NULL
            );
        """.trimIndent()
        )

        // Create output directory
        val outputDir = File(tempDir, "generated")
        outputDir.mkdirs()

        // Generate database files
        generateDatabaseFiles(
            dbName = "TestDatabase",
            sqlDir = tempDir,
            packageName = "com.test.db",
            outDir = outputDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        // Check generated content
        val generatedFiles = outputDir.walkTopDown()
            .filter { it.isFile && it.extension == "kt" }
            .toList()

        val databaseFile = generatedFiles.find { it.name.contains("TestDatabase") }
        assertTrue(databaseFile != null, "Database file should be generated")

        val generatedContent = databaseFile.readText()

        // Verify only sync table is included
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"sync_table\", syncKeyColumnName = \"custom_id\")"),
            "Should include sync table with custom primary key"
        )

        // Verify non-sync table is not included
        assertTrue(
            !generatedContent.contains("regular_table"),
            "Should not include non-sync table in syncTables"
        )
    }
}
