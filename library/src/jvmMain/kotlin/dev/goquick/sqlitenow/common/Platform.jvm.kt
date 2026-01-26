/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.common

import java.io.File

/**
 * Returns the path where the database should be stored on JVM platforms.
 * Uses OS app-data directories with an application-specific subdirectory.
 */
actual fun resolveDatabasePath(dbName: String, appName: String): String {
    val userHome = System.getProperty("user.home") ?: "."
    val osName = System.getProperty("os.name")?.lowercase() ?: ""

    val baseDir = when {
        osName.contains("mac") || osName.contains("darwin") ->
            File(userHome, "Library/Application Support")
        osName.contains("win") -> {
            val appData = System.getenv("APPDATA") ?: File(userHome, "AppData/Roaming").path
            File(appData)
        }
        else -> {
            val xdgDataHome = System.getenv("XDG_DATA_HOME")?.takeIf { it.isNotBlank() }
            File(xdgDataHome ?: File(userHome, ".local/share").path)
        }
    }

    val validatedAppName = requirePathSafeAppName(appName)

    val appDir = File(baseDir, validatedAppName)
    if (!appDir.exists()) {
        appDir.mkdirs()
    }

    return File(appDir, dbName).absolutePath
}

internal actual fun validateFileExists(path: String): Boolean {
    return File(path).exists()
}

actual fun platform(): PlatformType {
    return PlatformType.JVM
}

private fun requirePathSafeAppName(name: String): String {
    val trimmed = name.trim()
    require(trimmed.isNotEmpty()) {
        "appName must be a non-empty, path-safe string"
    }

    val invalidChars = Regex("""[\\/:*?"<>|]""")
    require(!invalidChars.containsMatchIn(trimmed)) {
        "appName contains path-unsafe characters: \\ / : * ? \" < > |"
    }

    return trimmed
}
