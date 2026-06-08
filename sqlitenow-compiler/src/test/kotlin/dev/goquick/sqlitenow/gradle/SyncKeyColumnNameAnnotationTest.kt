package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
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
        writeSchemaFiles(
            mapOf(
                "users.sql" to """
                    -- @@{enableSync=true, syncKeyColumnName=uuid}
                    CREATE TABLE users (
                        uuid TEXT PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL,
                        email TEXT UNIQUE NOT NULL
                    );
                """,
                "products.sql" to """
                    -- @@{enableSync=true}
                    CREATE TABLE products (
                        code TEXT PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL,
                        price REAL NOT NULL
                    );
                """,
                "blob_docs.sql" to """
                    -- @@{enableSync=true}
                    CREATE TABLE blob_docs (
                        id BLOB PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """,
            )
        )

        val generatedContent = generatedDatabaseContent()
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
        assertTrue(
            generatedContent.contains("fun buildOversqliteAutomaticDownloadConfig"),
            "Should generate automatic download config helper"
        )
        assertTrue(
            generatedContent.contains("bundleChangeWatchMode: BundleChangeWatchMode = BundleChangeWatchMode.OFF"),
            "Automatic download helper should default watch mode to OFF"
        )
        assertTrue(
            generatedContent.contains("OversqliteAutomaticDownloadConfig("),
            "Automatic download helper should build OversqliteAutomaticDownloadConfig"
        )
        assertTrue(
            generatedContent.contains("fun newOversqliteClient("),
            "Should generate newOversqliteClient helper for sync-enabled databases"
        )
        assertTrue(
            generatedContent.contains(
                "return DefaultOversqliteClient(db = this.connection(), config = cfg, http = httpClient, resolver = resolver)"
            ),
            "Generated oversqlite helper should bind directly to the database connection without callback glue"
        )
        assertTrue(
            !generatedContent.contains("tablesUpdateListener"),
            "Generated oversqlite helper should not emit tablesUpdateListener wiring"
        )
    }

    @TestFactory
    @DisplayName("invalid sync configurations fail closed")
    fun invalidSyncConfigurationsFailClosed(): List<DynamicTest> {
        return listOf(
            InvalidSyncConfigurationCase(
                displayName = "enableSync rejects unsupported integer primary keys",
                schemaFileName = "orders.sql",
                schemaSql = """
                    -- @@{enableSync=true}
                    CREATE TABLE orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        quantity INTEGER NOT NULL DEFAULT 1
                    );
                """,
                expectedMessage = "TEXT PRIMARY KEY or BLOB PRIMARY KEY"
            ),
            InvalidSyncConfigurationCase(
                displayName = "enableSync rejects unsupported bigint primary keys",
                schemaFileName = "events.sql",
                schemaSql = """
                    -- @@{enableSync=true}
                    CREATE TABLE events (
                        event_id BIGINT PRIMARY KEY NOT NULL,
                        title TEXT NOT NULL
                    );
                """,
                expectedMessage = "TEXT PRIMARY KEY or BLOB PRIMARY KEY"
            ),
            InvalidSyncConfigurationCase(
                displayName = "enableSync validation still runs when oversqlite bridge generation is disabled",
                schemaFileName = "orders.sql",
                schemaSql = """
                    -- @@{enableSync=true}
                    CREATE TABLE orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        quantity INTEGER NOT NULL DEFAULT 1
                    );
                """,
                oversqlite = false,
                expectedMessage = "TEXT PRIMARY KEY or BLOB PRIMARY KEY"
            ),
            InvalidSyncConfigurationCase(
                displayName = "oversqlite bridge requires at least one sync-enabled table",
                schemaFileName = "users.sql",
                schemaSql = """
                    CREATE TABLE users (
                        id TEXT PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """,
                oversqlite = true,
                expectedMessage = "no tables are annotated with enableSync=true"
            )
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                assertInvalidSyncConfiguration(case)
            }
        }
    }

    @Test
    @DisplayName("tables without enableSync annotation are not included in syncTables")
    fun testNonSyncTablesExcluded() {
        writeSchemaFiles(
            mapOf(
                "sync_table.sql" to """
                    -- @@{enableSync=true, syncKeyColumnName=custom_id}
                    CREATE TABLE sync_table (
                        custom_id TEXT PRIMARY KEY NOT NULL,
                        data TEXT NOT NULL
                    );
                """,
                "regular_table.sql" to """
                    CREATE TABLE regular_table (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        info TEXT NOT NULL
                    );
                """,
            )
        )

        val generatedContent = generatedDatabaseContent()
        assertTrue(
            generatedContent.contains("SyncTable(tableName = \"sync_table\", syncKeyColumnName = \"custom_id\")"),
            "Should include sync table with custom primary key"
        )
        assertTrue(
            !generatedContent.contains("regular_table"),
            "Should not include non-sync table in syncTables"
        )
    }

    @Test
    @DisplayName("oversqlite bridge is not generated unless explicitly enabled")
    fun oversqliteBridgeRequiresDslFlag() {
        writeSchemaFiles(
            mapOf(
                "users.sql" to """
                    -- @@{enableSync=true}
                    CREATE TABLE users (
                        id TEXT PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """,
            )
        )

        val generatedContent = generatedDatabaseContent(oversqlite = false)
        assertTrue(!generatedContent.contains("syncTables: List<SyncTable>"))
        assertTrue(!generatedContent.contains("fun buildOversqliteConfig"))
        assertTrue(!generatedContent.contains("fun buildOversqliteAutomaticDownloadConfig"))
        assertTrue(!generatedContent.contains("fun newOversqliteClient("))
    }

    private fun assertInvalidSyncConfiguration(case: InvalidSyncConfigurationCase) {
        val caseDir = File(tempDir, case.displayName.replace(Regex("[^A-Za-z0-9]+"), "_"))
        writeSchemaFiles(mapOf(case.schemaFileName to case.schemaSql), rootDir = caseDir)

        val outputDir = File(caseDir, "generated")
        outputDir.mkdirs()

        val error = assertFailsWith<IllegalArgumentException> {
            if (case.oversqlite == null) {
                generateDatabaseFiles(
                    dbName = "TestDatabase",
                    sqlDir = caseDir,
                    packageName = "com.test.db",
                    outDir = outputDir,
                    schemaDatabaseFile = null,
                    debug = false,
                )
            } else {
                generateDatabaseFiles(
                    dbName = "TestDatabase",
                    sqlDir = caseDir,
                    packageName = "com.test.db",
                    outDir = outputDir,
                    schemaDatabaseFile = null,
                    debug = false,
                    oversqlite = case.oversqlite,
                )
            }
        }

        assertTrue(error.message?.contains(case.expectedMessage) == true)
    }

    private fun writeSchemaFiles(
        files: Map<String, String>,
        rootDir: File = tempDir,
    ) {
        val schemaDir = File(rootDir, "schema")
        schemaDir.mkdirs()
        files.forEach { (name, sql) ->
            File(schemaDir, name).writeText(sql.trimIndent())
        }
    }

    private fun generatedDatabaseContent(
        rootDir: File = tempDir,
        oversqlite: Boolean = true,
    ): String {
        val outputDir = File(rootDir, "generated")
        outputDir.mkdirs()
        generateDatabaseFiles(
            dbName = "TestDatabase",
            sqlDir = rootDir,
            packageName = "com.test.db",
            outDir = outputDir,
            schemaDatabaseFile = null,
            debug = false,
            oversqlite = oversqlite,
        )

        val databaseFile = outputDir.walkTopDown()
            .first { it.isFile && it.name == "TestDatabase.kt" }
        return databaseFile.readText()
    }

    private data class InvalidSyncConfigurationCase(
        val displayName: String,
        val schemaFileName: String,
        val schemaSql: String,
        val expectedMessage: String,
        val oversqlite: Boolean? = null,
    )
}
