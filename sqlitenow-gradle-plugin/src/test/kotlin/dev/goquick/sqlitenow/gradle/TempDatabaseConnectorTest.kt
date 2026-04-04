package dev.goquick.sqlitenow.gradle

import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class TempDatabaseConnectorTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("File-backed temp connector replaces an existing file with a SQLite database")
    fun fileBackedConnectorReplacesExistingFile() {
        val databaseFile = tempDir.resolve("schema.db").toFile().apply {
            writeText("stale-data")
        }

        val connector = TempDatabaseConnector(MigratorTempStorage.File(databaseFile))
        connector.connection.createStatement().use { statement ->
            statement.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
        }
        connector.connection.commit()
        connector.close()

        assertTrue(databaseFile.exists(), "Database file should exist after creating the connector")
        assertFalse(databaseFile.readText().contains("stale-data"), "Previous file content should be replaced")
        assertEquals("SQLite format 3\u0000", databaseFile.inputStream().use { input ->
            ByteArray(16).also { input.read(it) }.toString(Charsets.US_ASCII)
        })
    }

    @Test
    @DisplayName("Temp connector enables foreign keys")
    fun tempConnectorEnablesForeignKeys() {
        val connector = TempDatabaseConnector(MigratorTempStorage.Memory)

        val foreignKeysEnabled = connector.connection.createStatement().use { statement ->
            statement.executeQuery("PRAGMA foreign_keys;").use { resultSet ->
                resultSet.next()
                resultSet.getInt(1)
            }
        }

        connector.close()

        assertEquals(1, foreignKeysEnabled, "PRAGMA foreign_keys should be enabled")
    }
}
