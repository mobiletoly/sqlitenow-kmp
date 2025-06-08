package dev.goquick.sqlitenow.core

import android.content.Context
import java.io.File

private lateinit var appContext: Context

fun setupAndroidAppContext(context: Context) {
    appContext = context
}

/**
 * Returns the path where the database should be stored on Android.
 * Uses the app's database directory.
 */
actual fun resolveDatabasePath(dbName: String): String {
    val path = appContext.getDatabasePath(dbName).absolutePath
    return path
}

actual fun validateFileExists(path: String): Boolean {
    return File(path).exists()
}
