package dev.goquick.sqlitenow.core

/**
 * Returns the path where the database should be stored on the current platform.
 */
expect fun resolveDatabasePath(dbName: String): String

/**
 * Checks if a file exists at the given path.
 */
internal expect fun validateFileExists(path: String): Boolean
