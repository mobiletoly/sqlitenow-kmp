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

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.toKString
import platform.posix.errno
import platform.posix.free
import platform.posix.readlink
import platform.posix.realpath

@OptIn(ExperimentalForeignApi::class)
internal fun canonicalSqliteNowDatabasePath(dbName: String): String {
    val resolvedDatabase = realpath(dbName, null)
    if (resolvedDatabase != null) {
        return try {
            resolvedDatabase.toKString()
        } finally {
            free(resolvedDatabase)
        }
    }

    val isSymbolicLink = memScoped {
        val probe = allocArray<ByteVar>(1)
        readlink(dbName, probe, 1.convert()) >= 0
    }
    if (isSymbolicLink) {
        throw SqliteNowException("SQLiteNow database path is a dangling symbolic link: $dbName")
    }

    val slash = dbName.lastIndexOf('/')
    val parent = when {
        slash < 0 -> "."
        slash == 0 -> "/"
        else -> dbName.substring(0, slash)
    }
    val fileName = dbName.substring(slash + 1)
    if (fileName.isEmpty()) {
        throw SqliteNowException("SQLiteNow database path has no final component: $dbName")
    }
    val resolvedParent = realpath(parent, null)
        ?: throw SqliteNowException("Cannot resolve SQLiteNow database parent $parent (errno=$errno)")
    return try {
        val canonicalParent = resolvedParent.toKString()
        if (canonicalParent == "/") "/$fileName" else "$canonicalParent/$fileName"
    } finally {
        free(resolvedParent)
    }
}
