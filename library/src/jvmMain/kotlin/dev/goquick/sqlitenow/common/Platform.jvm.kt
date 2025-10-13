/*
 * Copyright 2025 Anatoliy Pochkin
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

actual fun platform(): PlatformType {
    return PlatformType.JVM
}
