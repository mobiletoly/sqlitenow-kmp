@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

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

import kotlinx.cinterop.convert
import kotlinx.cinterop.toKString
import platform.posix.EEXIST
import platform.posix.F_OK
import platform.posix.access
import platform.posix.errno
import platform.posix.getenv
import platform.posix.mkdir

internal fun nativeUserHome(): String {
    return getenv("HOME")?.toKString()?.takeIf { it.isNotBlank() } ?: "."
}

internal fun nativeFileExists(path: String): Boolean = access(path, F_OK) == 0

internal fun resolveNativeDesktopDatabasePath(
    dbName: String,
    appName: String,
    baseDir: String,
): String {
    val validatedAppName = requirePathSafeAppName(appName)
    val appDir = joinPath(baseDir, validatedAppName)
    ensureDirectoryExists(appDir)
    return joinPath(appDir, dbName)
}

private fun ensureDirectoryExists(path: String) {
    val normalizedPath = path.trimEnd('/').ifEmpty { "/" }
    if (normalizedPath == "/") return

    val isAbsolute = normalizedPath.startsWith('/')
    val segments = normalizedPath.split('/').filter { it.isNotEmpty() }
    var currentPath = if (isAbsolute) "/" else ""

    for (segment in segments) {
        currentPath = when {
            currentPath.isEmpty() -> segment
            currentPath == "/" -> "/$segment"
            else -> "$currentPath/$segment"
        }

        if (nativeFileExists(currentPath)) continue
        if (mkdir(currentPath, 0x1FF.convert()) != 0 && errno != EEXIST) {
            error("Failed to create directory at $currentPath (errno=$errno)")
        }
    }
}

private fun joinPath(base: String, child: String): String {
    return when {
        base.isEmpty() -> child
        base.endsWith("/") -> "$base$child"
        else -> "$base/$child"
    }
}
