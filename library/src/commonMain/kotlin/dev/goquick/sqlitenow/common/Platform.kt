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

/**
 * Returns the path where the database should be stored on the current platform.
 *
 * @param dbName The database file name.
 * @param appName A stable, app-specific name used for JVM desktop paths. Ignored on non-JVM platforms.
 *        On JVM this value becomes part of the directory path, so keep it stable and path-safe.
 *        On JVM, path-unsafe characters (\\ / : * ? " < > |) will throw an IllegalArgumentException.
 */
expect fun resolveDatabasePath(dbName: String, appName: String): String

/**
 * Checks if a file exists at the given path.
 */
internal expect fun validateFileExists(path: String): Boolean

enum class PlatformType {
    JVM, ANDROID, IOS, JS
}

expect fun platform(): PlatformType
