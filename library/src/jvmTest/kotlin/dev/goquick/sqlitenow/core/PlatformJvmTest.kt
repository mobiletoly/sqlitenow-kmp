package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.common.resolveDatabasePath
import kotlin.test.Test
import kotlin.test.assertTrue
import java.io.File

class PlatformJvmTest {
    @Test
    fun testDatabasePath() {
        val dbName = "test_database.db"
        val appName = "SqliteNowTest"
        val path = resolveDatabasePath(dbName, appName)

        // Check that the path contains the database name
        assertTrue(path.contains(dbName), "Path should contain the database name")

        // Check that the path contains the app-specific directory
        assertTrue(path.contains(appName), "Path should contain the app-specific directory")

        // Check that the path uses an OS-appropriate base directory
        val osName = System.getProperty("os.name")?.lowercase() ?: ""
        val userHome = System.getProperty("user.home") ?: "."
        val expectedBaseDir = when {
            osName.contains("mac") || osName.contains("darwin") ->
                File(userHome, "Library/Application Support").path
            osName.contains("win") -> {
                val appData = System.getenv("APPDATA") ?: File(userHome, "AppData/Roaming").path
                File(appData).path
            }
            else -> {
                val xdgDataHome = System.getenv("XDG_DATA_HOME")?.takeIf { it.isNotBlank() }
                File(xdgDataHome ?: File(userHome, ".local/share").path).path
            }
        }
        assertTrue(path.startsWith(expectedBaseDir), "Path should start with the OS app-data directory")
        
        // Check that the directory exists
        val file = File(path)
        assertTrue(file.parentFile.exists(), "Database directory should exist")
    }
}
