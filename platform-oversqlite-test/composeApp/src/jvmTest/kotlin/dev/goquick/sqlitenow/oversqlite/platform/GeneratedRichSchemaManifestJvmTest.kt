package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.VersionBasedDatabaseMigrations
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals

class GeneratedRichSchemaManifestJvmTest {
    @Test
    fun generatedRealServerDatabaseMatchesBusinessRichManifest() = runTest {
        val manifest = testJson.decodeFromString(
            RichSchemaManifest.serializer(),
            richSchemaManifestFile().readText(),
        )
        assertEquals(1, manifest.formatVersion)
        assertEquals("business-rich-v0", manifest.fixture)
        assertEquals("business", manifest.schema)

        val expectedSyncTables = manifest.tables
            .map { it.name to it.syncKeyColumnName }
            .sortedBy { it.first }
        val actualSyncTables = RealServerGeneratedDatabase.syncTables
            .map { it.tableName to it.syncKeyColumnName }
            .sortedBy { it.first }
        assertEquals(expectedSyncTables, actualSyncTables)

        val database = RealServerGeneratedDatabase(
            dbName = ":memory:",
            migration = VersionBasedDatabaseMigrations(),
            debug = true,
        )
        try {
            database.open()
            val connection = database.connection()
            for (table in manifest.tables) {
                assertTableMatchesManifest(connection, table)
            }
        } finally {
            database.close()
        }
    }

    private suspend fun assertTableMatchesManifest(
        connection: SafeSQLiteConnection,
        table: RichSchemaTable,
    ) {
        val columns = readColumns(connection, table.name)
        val expectedColumns = table.columns.map {
            ManifestColumn(
                name = it.name,
                logicalType = it.logicalType,
                nullable = it.nullable,
            )
        }
        assertEquals(expectedColumns, columns.map { it.manifestColumn }, "${table.name}: columns")
        assertEquals(table.primaryKey, columns.filter { it.primaryKeyIndex > 0 }.sortedBy { it.primaryKeyIndex }.map { it.name })

        val expectedForeignKeys = table.foreignKeys.sortedWith(compareBy({ it.from }, { it.toTable }, { it.toColumn }))
        val actualForeignKeys = readForeignKeys(connection, table.name)
            .sortedWith(compareBy({ it.from }, { it.toTable }, { it.toColumn }))
        assertEquals(expectedForeignKeys, actualForeignKeys, "${table.name}: foreign keys")
    }

    private suspend fun readColumns(
        connection: SafeSQLiteConnection,
        tableName: String,
    ): List<ActualColumn> {
        return connection.withExclusiveAccess {
            connection.prepare("PRAGMA table_info($tableName)").use { statement ->
                val columns = mutableListOf<ActualColumn>()
                while (statement.step()) {
                    val type = statement.getText(2)
                    columns += ActualColumn(
                        name = statement.getText(1),
                        manifestColumn = ManifestColumn(
                            name = statement.getText(1),
                            logicalType = sqliteTypeToLogicalType(type),
                            nullable = statement.getLong(3) == 0L,
                        ),
                        primaryKeyIndex = statement.getLong(5).toInt(),
                    )
                }
                columns
            }
        }
    }

    private suspend fun readForeignKeys(
        connection: SafeSQLiteConnection,
        tableName: String,
    ): List<ManifestForeignKey> {
        return connection.withExclusiveAccess {
            connection.prepare("PRAGMA foreign_key_list($tableName)").use { statement ->
                val foreignKeys = mutableListOf<ManifestForeignKey>()
                while (statement.step()) {
                    foreignKeys += ManifestForeignKey(
                        from = statement.getText(3),
                        toTable = statement.getText(2),
                        toColumn = statement.getText(4),
                        onDelete = statement.getText(6).uppercase(),
                    )
                }
                foreignKeys
            }
        }
    }

    private fun sqliteTypeToLogicalType(type: String): String =
        when (type.trim().uppercase()) {
            "TEXT" -> "text"
            "BLOB" -> "blob"
            "INTEGER" -> "integer"
            "REAL" -> "real"
            else -> type.trim().lowercase()
        }

    private fun richSchemaManifestFile(): Path =
        findRepoRoot().resolve("oversqlite-contracts/rich-schema/business-rich-v0.json")

    private fun findRepoRoot(): Path {
        var current = Paths.get("").toAbsolutePath()
        while (true) {
            if (current.resolve("settings.gradle.kts").exists()) {
                return current
            }
            current = current.parent
                ?: error("could not locate repository root from ${Paths.get("").toAbsolutePath()}")
        }
    }

    @Serializable
    private data class RichSchemaManifest(
        val formatVersion: Int,
        val fixture: String,
        val schema: String,
        val tables: List<RichSchemaTable>,
    )

    @Serializable
    private data class RichSchemaTable(
        val name: String,
        val syncKeyColumnName: String,
        val primaryKey: List<String>,
        val columns: List<ManifestColumn>,
        val foreignKeys: List<ManifestForeignKey>,
    )

    @Serializable
    private data class ManifestColumn(
        val name: String,
        val logicalType: String,
        val nullable: Boolean,
    )

    @Serializable
    private data class ManifestForeignKey(
        val from: String,
        val toTable: String,
        val toColumn: String,
        val onDelete: String,
    )

    private data class ActualColumn(
        val name: String,
        val manifestColumn: ManifestColumn,
        val primaryKeyIndex: Int,
    )
}
