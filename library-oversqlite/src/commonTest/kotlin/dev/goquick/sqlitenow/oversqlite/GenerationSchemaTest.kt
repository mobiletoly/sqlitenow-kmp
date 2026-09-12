package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotEquals

internal class GenerationSchemaTest : CrossTargetSyncTestSupport() {
    private val keys = mapOf("parent" to listOf("id"), "child" to listOf("id"))
    private val generation = SyncGeneration("1", "unused by physical validation", listOf(
        GenerationTable("alpha", "parent", listOf("id"), listOf(GenerationColumn("id", "uuid", false)), emptyList()),
        GenerationTable("beta", "child", listOf("id"), listOf(
            GenerationColumn("id", "uuid", false), GenerationColumn("parent_id", "uuid", true),
        ), listOf(GenerationForeignKey(listOf("parent_id"), "alpha", "parent", listOf("id"), "set_null", "no_action"))),
    ))

    @Test
    fun metadataAcceptsAllForeignKeyTimingSpellings() = runOwnedWebDatabaseTest {
        val scenarios = mapOf(
            "plain" to "", "not deferrable" to "NOT DEFERRABLE",
            "deferred" to "DEFERRABLE INITIALLY DEFERRED", "immediate" to "DEFERRABLE INITIALLY IMMEDIATE",
        )
        for ((name, timing) in scenarios) {
            val db = newDb()
            try {
                db.execSQL("CREATE TABLE parent(id BLOB PRIMARY KEY NOT NULL)")
                db.execSQL("CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id BLOB REFERENCES parent(id) ON DELETE SET NULL $timing)")
                val cache = TableInfoCache()
                val tables = mapOf("parent" to cache.get(db, "parent"), "child" to cache.get(db, "child"))
                generation.requireMatchingLocalSchema(tables, keys)
                assertEquals("SET NULL", tables.getValue("child").foreignKeys.single().onDelete, name)
            } finally { db.close() }
        }
    }

    @Test
    fun rejectsPhysicalMismatchesIncludingGeneratedColumnsAndViews() = runOwnedWebDatabaseTest {
        val scenarios = mapOf(
            "wrong action" to "CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id BLOB REFERENCES parent(id) ON DELETE CASCADE)",
            "missing foreign key" to "CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id BLOB)",
            "extra column" to "CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id BLOB REFERENCES parent(id) ON DELETE SET NULL, extra TEXT)",
            "wrong affinity" to "CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
            "uuid foreign key representation mismatch" to "CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id TEXT REFERENCES parent(id) ON DELETE SET NULL)",
            "wrong nullability" to "CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id BLOB NOT NULL REFERENCES parent(id) ON DELETE SET NULL)",
            "generated column" to "CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id BLOB GENERATED ALWAYS AS (id) REFERENCES parent(id) ON DELETE SET NULL)",
            "composite key" to "CREATE TABLE child(id BLOB NOT NULL, parent_id BLOB REFERENCES parent(id) ON DELETE SET NULL, PRIMARY KEY(id,parent_id))",
            "view" to "CREATE VIEW child AS SELECT id, id AS parent_id FROM parent",
            "wrong case" to "CREATE TABLE child(ID BLOB PRIMARY KEY NOT NULL, parent_id BLOB REFERENCES parent(id) ON DELETE SET NULL)",
        )
        for ((name, sql) in scenarios) {
            val db = newDb()
            try {
                db.execSQL("CREATE TABLE parent(id BLOB PRIMARY KEY NOT NULL)")
                db.execSQL(sql)
                val cache = TableInfoCache()
                val tables = mapOf("parent" to cache.get(db, "parent"), "child" to cache.get(db, "child"))
                assertFails(name) { generation.requireMatchingLocalSchema(tables, keys) }
            } finally { db.close() }
        }
    }

    @Test
    fun retainsPhysicalShapeAndRequiresCompleteUnambiguousTableMapping() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE parent(id BLOB PRIMARY KEY NOT NULL)")
            db.execSQL("CREATE TABLE child(id BLOB PRIMARY KEY NOT NULL, parent_id BLOB REFERENCES parent(id) ON DELETE SET NULL)")
            val cache = TableInfoCache()
            val tables = mapOf("parent" to cache.get(db, "parent"), "child" to cache.get(db, "child"))
            generation.requireMatchingLocalSchema(tables, keys)
            assertFails { generation.requireMatchingLocalSchema(tables - "child", keys - "child") }
            val collision = generation.copy(tables = listOf(generation.tables.first(), generation.tables.first().copy(schema = "beta")))
            assertFails { collision.requireMatchingLocalSchema(tables, keys) }
            val shape = managedPhysicalShape(tables, keys)
            db.execSQL("ALTER TABLE child ADD COLUMN extra TEXT")
            cache.clear()
            val changed = tables + ("child" to cache.get(db, "child"))
            assertNotEquals(shape, managedPhysicalShape(changed, keys))
        } finally { db.close() }
    }

    @Test
    fun affinityUsesSQLitePrecedenceAndRejectsNumericForExactDecimal() {
        val cases = mapOf("INTEGER" to "INTEGER", "FLOATING POINT" to "INTEGER", "CHARBLOB" to "TEXT", "" to "BLOB", "DOUBLE" to "REAL", "DECIMAL" to "NUMERIC")
        for ((declared, expected) in cases) assertEquals(expected, sqliteAffinity(declared), declared)
        val decimal = GenerationColumn("amount", "decimal", false)
        decimal.requireCompatibleStorage(ColumnInfo("amount", "TEXT", false, true, null))
        assertFails { decimal.requireCompatibleStorage(ColumnInfo("amount", "REAL", false, true, null)) }
        assertFails { decimal.requireCompatibleStorage(ColumnInfo("amount", "DECIMAL", false, true, null)) }
    }
}
