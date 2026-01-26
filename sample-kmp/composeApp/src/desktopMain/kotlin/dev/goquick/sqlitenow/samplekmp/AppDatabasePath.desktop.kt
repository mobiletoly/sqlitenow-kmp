/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.goquick.sqlitenow.samplekmp

import java.io.File

private const val DESKTOP_APP_DIR = "SampleKmp"

actual fun appDatabasePath(dbName: String): String {
    val osName = System.getProperty("os.name")?.lowercase() ?: ""
    val homeDir = System.getProperty("user.home") ?: "."

    val baseDir = when {
        osName.contains("mac") || osName.contains("darwin") ->
            File(homeDir, "Library/Application Support")
        osName.contains("win") -> {
            val appData = System.getenv("APPDATA") ?: File(homeDir, "AppData/Roaming").path
            File(appData)
        }
        else -> {
            val xdgDataHome = System.getenv("XDG_DATA_HOME")?.takeIf { it.isNotBlank() }
            File(xdgDataHome ?: File(homeDir, ".local/share").path)
        }
    }

    val appDir = File(baseDir, DESKTOP_APP_DIR)
    if (!appDir.exists()) {
        appDir.mkdirs()
    }

    return File(appDir, dbName).path
}
