package dev.goquick.sqlitenow.common

/**
 * Returns the path where the database should be stored on the current platform.
 */
expect fun resolveDatabasePath(dbName: String): String

/**
 * Checks if a file exists at the given path.
 */
internal expect fun validateFileExists(path: String): Boolean

enum class PlatformType {
    JVM, ANDROID, IOS, JS
}

expect fun platform(): PlatformType
