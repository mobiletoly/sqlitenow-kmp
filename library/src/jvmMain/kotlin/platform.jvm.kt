package dev.goquick.sqlitenow.core

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Returns the path where the database should be stored on JVM platforms.
 * Uses the user's home directory with a subdirectory for the application.
 */
actual fun resolveDatabasePath(dbName: String): String {
    // Get the user's home directory
    val userHome = System.getProperty("user.home")
    
    // Create an application-specific directory
    val appDir = File(userHome, ".sqlitenow")
    
    // Ensure the directory exists
    if (!appDir.exists()) {
        appDir.mkdirs()
    }
    
    // Return the full path to the database file
    return File(appDir, dbName).absolutePath
}

actual fun validateFileExists(path: String): Boolean {
    return File(path).exists()
}
