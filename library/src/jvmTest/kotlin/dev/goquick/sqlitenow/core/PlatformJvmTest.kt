package dev.goquick.sqlitenow.core

import kotlin.test.Test
import kotlin.test.assertTrue
import java.io.File

class PlatformJvmTest {
    @Test
    fun testDatabasePath() {
        val dbName = "test_database.db"
        val path = resolveDatabasePath(dbName)
        
        // Check that the path contains the database name
        assertTrue(path.contains(dbName), "Path should contain the database name")
        
        // Check that the path contains the .sqlitenow directory
        assertTrue(path.contains(".sqlitenow"), "Path should contain the .sqlitenow directory")
        
        // Check that the directory exists
        val file = File(path)
        assertTrue(file.parentFile.exists(), "Database directory should exist")
    }
}
