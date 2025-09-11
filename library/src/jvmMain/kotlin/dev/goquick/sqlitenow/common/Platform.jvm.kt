package dev.goquick.sqlitenow.common

import java.io.File

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

internal actual fun validateFileExists(path: String): Boolean {
    return File(path).exists()
}

internal actual fun platform(): PlatformType {
    return PlatformType.JVM
}
