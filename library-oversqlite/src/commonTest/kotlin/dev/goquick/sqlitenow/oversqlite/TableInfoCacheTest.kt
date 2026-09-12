package dev.goquick.sqlitenow.oversqlite

import kotlin.test.assertEquals
import kotlin.test.Test

internal class TableInfoCacheTest : CrossTargetSyncTestSupport() {
    private data class TableInfoScenario(
        val name: String,
        val table: String,
        val schema: String,
        val columnNames: List<String>,
        val types: Map<String, String>,
        val primaryKey: String,
        val primaryKeyIsBlob: Boolean,
        val verifyRepeatedRead: Boolean = false,
    )

    @Test
    fun parses_named_table_schema_scenarios() = runOwnedWebDatabaseTest {
        val scenarios = listOf(
            TableInfoScenario(
                name = "regular text primary key",
                table = "ext_users",
                schema = """
                CREATE TABLE ext_users (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  email TEXT UNIQUE,
                  avatar BLOB,
                  created_at TEXT DEFAULT (datetime('now'))
                )
                """.trimIndent(),
                columnNames = listOf("id", "name", "email", "avatar", "created_at"),
                types = mapOf(
                    "id" to "text",
                    "name" to "text",
                    "email" to "text",
                    "avatar" to "blob",
                    "created_at" to "text",
                ),
                primaryKey = "id",
                primaryKeyIsBlob = false,
            ),
            TableInfoScenario(
                name = "blob primary key",
                table = "blobs",
                schema = """
                CREATE TABLE blobs (
                  id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
                  note TEXT
                )
                """.trimIndent(),
                columnNames = listOf("id", "note"),
                types = mapOf("id" to "blob", "note" to "text"),
                primaryKey = "id",
                primaryKeyIsBlob = true,
                verifyRepeatedRead = true,
            ),
        )

        scenarios.forEach { scenario ->
            val db = newDb()
            val cache = TableInfoCache()
            try {
                db.execSQL(scenario.schema)

                val info = cache.get(db, scenario.table)
                assertEquals(scenario.table, info.table, scenario.name)
                assertEquals(scenario.columnNames, info.columnNamesLower, scenario.name)
                scenario.types.forEach { (column, type) ->
                    assertEquals(type, info.typesByNameLower[column]?.lowercase(), "${scenario.name}: $column")
                }
                assertEquals(scenario.primaryKey, info.primaryKey?.name?.lowercase(), scenario.name)
                assertEquals(scenario.primaryKeyIsBlob, info.primaryKeyIsBlob, scenario.name)

                if (scenario.verifyRepeatedRead) {
                    val repeated = cache.get(db, scenario.table)
                    assertEquals(info.columnNamesLower, repeated.columnNamesLower, scenario.name)
                    assertEquals(info.typesByNameLower, repeated.typesByNameLower, scenario.name)
                    assertEquals(info.primaryKeyIsBlob, repeated.primaryKeyIsBlob, scenario.name)
                }
            } finally {
                db.close()
            }
        }
    }

    @Test
    fun cacheEntriesAreIsolatedAcrossConnectionsAndCacheOwners() = runOwnedWebDatabaseTest {
        val first = newDb()
        val second = newDb()
        try {
            first.execSQL("CREATE TABLE shared(id TEXT PRIMARY KEY, first_value INTEGER NOT NULL)")
            second.execSQL("CREATE TABLE shared(id BLOB PRIMARY KEY, second_value TEXT NOT NULL)")
            val firstCache = TableInfoCache()
            val secondCache = TableInfoCache()

            val firstInfo = firstCache.get(first, "shared")
            val secondInfo = secondCache.get(second, "shared")

            assertEquals(listOf("id", "first_value"), firstInfo.columnNamesLower)
            assertEquals(false, firstInfo.primaryKeyIsBlob)
            assertEquals(listOf("id", "second_value"), secondInfo.columnNamesLower)
            assertEquals(true, secondInfo.primaryKeyIsBlob)
            assertEquals(firstInfo, firstCache.get(first, "shared"))
            assertEquals(secondInfo, secondCache.get(second, "shared"))
        } finally {
            first.close()
            second.close()
        }
    }
}
