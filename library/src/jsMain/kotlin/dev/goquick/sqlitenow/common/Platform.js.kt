package dev.goquick.sqlitenow.common

actual fun resolveDatabasePath(dbName: String): String = dbName

internal actual fun validateFileExists(path: String): Boolean = false

actual fun platform(): PlatformType = PlatformType.JS
