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
package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection

@Suppress("UNUSED_PARAMETER")
internal actual suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    initialBytes: ByteArray?,
    config: SqliteConnectionConfig,
): SQLiteConnection {
    requireSupportedJvmHost(
        osName = System.getProperty("os.name"),
        osArch = System.getProperty("os.arch"),
    )
    return openBundledSqliteDriverConnection(dbName)
}

internal fun requireSupportedJvmHost(osName: String, osArch: String) {
    val normalizedOs = osName.lowercase()
    val normalizedArch = osArch.lowercase()
    val isMac = normalizedOs.contains("mac") || normalizedOs.contains("darwin")
    val isArm64 = normalizedArch.contains("aarch64") || normalizedArch.contains("arm64")
    if (isMac && !isArm64) {
        throw UnsupportedOperationException(
            "SQLiteNow's JVM runtime does not support Intel macOS. " +
                "AndroidX SQLite 2.7 does not provide the required osx_x64 JNI payload.",
        )
    }
}
