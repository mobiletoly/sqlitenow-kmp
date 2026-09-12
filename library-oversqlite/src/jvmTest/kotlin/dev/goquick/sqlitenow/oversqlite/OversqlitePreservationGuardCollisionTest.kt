package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotEquals
import kotlinx.coroutines.test.runTest

class OversqlitePreservationGuardCollisionTest {
    @Test
    fun realValuesMatchSharedCompleteWitness() = runTest {
        val fixture = oversqliteContractFixture("preservation/real-witness-v1.sql")
        val lines = Files.readAllLines(fixture)
        val expected = lines.first().substringAfter("expected_digest:").trim()
        val path = Files.createTempFile("oversqlite-real-witness", ".db").toString()
        val connection = BundledSqliteConnectionProvider.openConnection(path, debug = false)
        try {
            lines.drop(1).filter(String::isNotBlank).forEach { connection.execSQL(it.removeSuffix(";")) }
            assertEquals(expected, preservedStateDigest(connection, setOf("real_witness")))
        } finally {
            connection.close()
            Files.deleteIfExists(java.nio.file.Path.of(path))
        }
    }

    @Test
    fun applicationPreservationPrefixTriggerRemainsInWitness() = runTest {
        val path = Files.createTempFile("oversqlite-preservation-prefix", ".db").toString()
        val connection = BundledSqliteConnectionProvider.openConnection(path, debug = false)
        try {
            connection.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY)")
            connection.execSQL(
                "CREATE TRIGGER _sqlitenow_preserve_application " +
                    "AFTER INSERT ON notes BEGIN SELECT 1; END",
            )

            val before = preservedStateDigest(connection, setOf("notes"))
            connection.execSQL("DROP TRIGGER _sqlitenow_preserve_application")
            val after = preservedStateDigest(connection, setOf("notes"))

            assertNotEquals(before, after)
        } finally {
            connection.close()
            Files.deleteIfExists(java.nio.file.Path.of(path))
        }
    }

    @Test
    fun edgeCasesMatchSharedCompleteWitness() = runTest {
        val fixture = oversqliteContractFixture("preservation/edge-cases-v1.sql")
        val lines = Files.readAllLines(fixture)
        val expected = lines.first().substringAfter("expected_digest:").trim()
        val path = Files.createTempFile("oversqlite-edge-witness", ".db").toString()
        val connection = BundledSqliteConnectionProvider.openConnection(path, debug = false)
        try {
            lines.drop(1).filter(String::isNotBlank).forEach { connection.execSQL(it.removeSuffix(";")) }
            assertEquals(
                expected,
                preservedStateDigest(connection, setOf("keyless_values", "without_rowid_values")),
            )
        } finally {
            connection.close()
            Files.deleteIfExists(java.nio.file.Path.of(path))
        }
    }

    @Test
    fun preservationGuardsRollbackIndirectForeignKeyAndTriggerMutations() = runTest {
        val path = Files.createTempFile("oversqlite-indirect-preservation", ".db").toString()
        val connection = BundledSqliteConnectionProvider.openConnection(path, debug = false)
        try {
            connection.execSQL("PRAGMA foreign_keys=ON")
            connection.execSQL("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            connection.execSQL(
                "CREATE TABLE preserved(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE,value TEXT)",
            )
            connection.execSQL("CREATE TABLE driver(id INTEGER PRIMARY KEY,value TEXT)")
            connection.execSQL(
                "CREATE TRIGGER driver_mutation AFTER UPDATE ON driver BEGIN " +
                    "UPDATE preserved SET value='triggered' WHERE id=1; END",
            )
            connection.execSQL("INSERT INTO parent VALUES(1)")
            connection.execSQL("INSERT INTO preserved VALUES(1,1,'kept')")
            connection.execSQL("INSERT INTO driver VALUES(1,'idle')")
            installPreservedLocalTableGuards(connection, setOf("preserved"))

            assertFails { connection.execSQL("DELETE FROM parent WHERE id=1") }
            assertEquals(1L, connection.scalarLong("SELECT count(*) FROM parent WHERE id=1"))
            assertEquals("kept", connection.scalarText("SELECT value FROM preserved WHERE id=1"))

            assertFails { connection.execSQL("UPDATE driver SET value='go' WHERE id=1") }
            assertEquals("idle", connection.scalarText("SELECT value FROM driver WHERE id=1"))
            assertEquals("kept", connection.scalarText("SELECT value FROM preserved WHERE id=1"))
        } finally {
            connection.close()
            Files.deleteIfExists(java.nio.file.Path.of(path))
        }
    }
}

private suspend fun dev.goquick.sqlitenow.core.SafeSQLiteConnection.scalarText(sql: String): String =
    queryList(sql) { it.getText(0) }.single()
