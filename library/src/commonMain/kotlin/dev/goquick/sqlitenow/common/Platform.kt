package dev.goquick.sqlitenow.common

/**
 * Returns the path where the database should be stored on the current platform.
 */
expect fun resolveDatabasePath(dbName: String): String

/**
 * Checks if a file exists at the given path.
 */
internal expect fun validateFileExists(path: String): Boolean

internal enum class PlatformType {
    JVM, ANDROID, IOS
}

internal expect fun platform(): PlatformType
