package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals

internal class SharedRichSchemaManifestFixtureTest : RealServerSupport() {
    @Test
    fun kmpRichRuntimeSchemaMatchesSharedManifest() = runTest {
        val manifest = json.decodeFromString(
            RichSchemaManifest.serializer(),
            oversqliteContractFixture("rich-schema/business-rich-v0.json").readText(),
        )
        assertEquals(1, manifest.formatVersion)
        assertEquals("business-rich-v0", manifest.fixture)
        assertEquals("business", manifest.schema)
        assertEquals(
            listOf(
                "signed-64-min",
                "signed-64-max",
                "above-javascript-safe-range",
                "binary64-negative-zero",
                "binary64-subnormal",
                "binary64-ordinary",
                "binary64-maximum-finite",
                "postgres-float4-authoritative-spelling",
                "boolean-false",
                "boolean-true",
            ),
            manifest.numericScenarios.map { it.name },
        )

        val expectedSyncTables = manifest.tables
            .map { it.name to it.syncKeyColumnName }
            .sortedBy { it.first }
        val actualSyncTables = businessRichSyncTables
            .map { it.tableName to it.syncKeyColumnName }
            .sortedBy { it.first }
        assertEquals(expectedSyncTables, actualSyncTables)

        val db = newDb()
        try {
            createBusinessRichSchemaTables(db)
            for (table in manifest.tables) {
                assertTableMatchesManifest(db, table)
            }
        } finally {
            db.close()
        }
    }

    private suspend fun assertTableMatchesManifest(
        db: SafeSQLiteConnection,
        table: RichSchemaTable,
    ) {
        val columns = readColumns(db, table.name)
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
        val actualForeignKeys = readForeignKeys(db, table.name)
            .sortedWith(compareBy({ it.from }, { it.toTable }, { it.toColumn }))
        assertEquals(expectedForeignKeys, actualForeignKeys, "${table.name}: foreign keys")
    }

    private suspend fun readColumns(
        db: SafeSQLiteConnection,
        tableName: String,
    ): List<ActualColumn> {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA table_info($tableName)").use { statement ->
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
        db: SafeSQLiteConnection,
        tableName: String,
    ): List<ManifestForeignKey> {
        return db.withExclusiveAccess {
            db.prepare("PRAGMA foreign_key_list($tableName)").use { statement ->
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

    @Serializable
    private data class RichSchemaManifest(
        val formatVersion: Int,
        val fixture: String,
        val schema: String,
        val tables: List<RichSchemaTable>,
        val numericScenarios: List<NumericScenario>,
    )

    @Serializable
    private data class NumericScenario(
        val name: String,
        val local: Map<String, String>,
        val committed: Map<String, String>,
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
