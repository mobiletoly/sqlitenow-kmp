package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SyncKeyColumnNameAnnotationTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    @DisplayName("enableSync tables emit explicit SyncTable metadata for text and blob primary keys")
    fun syncKeyColumnNameAnnotation_generatesExplicitSyncTables() {
        val schemaDir = File(tempDir, "schema")
        schemaDir.mkdirs()

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

        File(schemaDir, "blob_docs.sql").writeText(
            """
            -- @@{enableSync=true}
            CREATE TABLE blob_docs (
                id BLOB PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
            """.trimIndent()
        )

        val outputDir = File(tempDir, "generated")
        outputDir.mkdirs()

        generateDatabaseFiles(
            dbName = "TestDatabase",
            sqlDir = tempDir,
            packageName = "com.test.db",
            outDir = outputDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        val generatedFiles = outputDir.walkTopDown()
            .filter { it.isFile && it.extension == "kt" }
            .toList()
        val databaseFile = generatedFiles.find { it.name.contains("TestDatabase") }
        assertTrue(databaseFile != null, "Database file should be generated")

        val generatedContent = databaseFile.readText()
        assertTrue(
            generatedContent.contains("syncTables: List<SyncTable>"),
            "Should generate syncTables property with correct type"
        )
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"users\", syncKeyColumnName = \"uuid\")"),
            "Should use explicit syncKeyColumnName from annotation"
        )
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"products\", syncKeyColumnName = \"code\")"),
            "Should auto-detect primary key column name"
        )
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"blob_docs\", syncKeyColumnName = \"id\")"),
            "Should allow blob primary key sync tables"
        )
        assertTrue(
            generatedContent.contains("fun buildOversqliteConfig"),
            "Should generate buildOversqliteConfig function"
        )
        assertTrue(
            generatedContent.contains("OversqliteConfig(schema, syncTables"),
            "buildOversqliteConfig should use syncTables parameter"
        )
    }

    @Test
    @DisplayName("enableSync rejects unsupported integer primary keys")
    fun syncKeyColumnNameAnnotation_rejectsIntegerSyncTables() {
        val schemaDir = File(tempDir, "schema")
        schemaDir.mkdirs()

        File(schemaDir, "orders.sql").writeText(
            """
            -- @@{enableSync=true}
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quantity INTEGER NOT NULL DEFAULT 1
            );
            """.trimIndent()
        )

        val outputDir = File(tempDir, "generated")
        outputDir.mkdirs()

        val error = assertFailsWith<IllegalArgumentException> {
            generateDatabaseFiles(
                dbName = "TestDatabase",
                sqlDir = tempDir,
                packageName = "com.test.db",
                outDir = outputDir,
                schemaDatabaseFile = null,
                debug = false,
            )
        }

        assertTrue(
            error.message?.contains("TEXT PRIMARY KEY or BLOB PRIMARY KEY") == true,
            "Should reject integer sync primary keys with a clear error"
        )
    }

    @Test
    @DisplayName("enableSync rejects unsupported bigint primary keys")
    fun syncKeyColumnNameAnnotation_rejectsBigIntSyncTables() {
        val schemaDir = File(tempDir, "schema")
        schemaDir.mkdirs()

        File(schemaDir, "events.sql").writeText(
            """
            -- @@{enableSync=true}
            CREATE TABLE events (
                event_id BIGINT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL
            );
            """.trimIndent()
        )

        val outputDir = File(tempDir, "generated")
        outputDir.mkdirs()

        val error = assertFailsWith<IllegalArgumentException> {
            generateDatabaseFiles(
                dbName = "TestDatabase",
                sqlDir = tempDir,
                packageName = "com.test.db",
                outDir = outputDir,
                schemaDatabaseFile = null,
                debug = false,
            )
        }

        assertTrue(
            error.message?.contains("TEXT PRIMARY KEY or BLOB PRIMARY KEY") == true,
            "Should reject bigint sync primary keys with a clear error"
        )
    }

    @Test
    @DisplayName("tables without enableSync annotation are not included in syncTables")
    fun testNonSyncTablesExcluded() {
        val schemaDir = File(tempDir, "schema")
        schemaDir.mkdirs()

        File(schemaDir, "sync_table.sql").writeText(
            """
            -- @@{enableSync=true, syncKeyColumnName=custom_id}
            CREATE TABLE sync_table (
                custom_id TEXT PRIMARY KEY NOT NULL,
                data TEXT NOT NULL
            );
            """.trimIndent()
        )

        File(schemaDir, "regular_table.sql").writeText(
            """
            CREATE TABLE regular_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                info TEXT NOT NULL
            );
            """.trimIndent()
        )

        val outputDir = File(tempDir, "generated")
        outputDir.mkdirs()

        generateDatabaseFiles(
            dbName = "TestDatabase",
            sqlDir = tempDir,
            packageName = "com.test.db",
            outDir = outputDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        val generatedFiles = outputDir.walkTopDown()
            .filter { it.isFile && it.extension == "kt" }
            .toList()
        val databaseFile = generatedFiles.find { it.name.contains("TestDatabase") }
        assertTrue(databaseFile != null, "Database file should be generated")

        val generatedContent = databaseFile.readText()
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"sync_table\", syncKeyColumnName = \"custom_id\")"),
            "Should include sync table with custom primary key"
        )
        assertTrue(
            !generatedContent.contains("regular_table"),
            "Should not include non-sync table in syncTables"
        )
    }
}
