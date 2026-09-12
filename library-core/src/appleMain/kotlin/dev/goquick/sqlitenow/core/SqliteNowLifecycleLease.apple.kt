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
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import platform.posix.LOCK_EX
import platform.posix.LOCK_NB
import platform.posix.O_CREAT
import platform.posix.O_RDWR
import platform.posix.close
import platform.posix.errno
import platform.posix.flock
import platform.posix.open

private object AppleLifecycleOwners {
    val mutex = Mutex()
    val identities = mutableSetOf<String>()
}

internal actual fun sqliteNowActiveClientIdentity(dbName: String, connection: SQLiteConnection): Any =
    if (dbName.isAppleInMemoryPath()) Any() else canonicalSqliteNowDatabasePath(dbName)

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun acquireSqliteNowLifecycleLease(dbName: String): SqliteNowLifecycleLease {
    val inMemory = dbName.isAppleInMemoryPath()
    if (inMemory) {
        return appleLifecycleLease("memory:$dbName", -1, registered = false)
    }
    val identity = canonicalSqliteNowDatabasePath(dbName)
    AppleLifecycleOwners.mutex.withLock {
        if (!AppleLifecycleOwners.identities.add(identity)) {
            throw SqliteNowLifecycleBusyException(identity)
        }
    }
    val sidecar = "$identity.sqlitenow-opening-v1.lock"
    val fd = open(sidecar, O_CREAT or O_RDWR, 0x180)
    if (fd < 0) {
        AppleLifecycleOwners.mutex.withLock { AppleLifecycleOwners.identities.remove(identity) }
        throw SqliteNowException("Cannot open SQLiteNow lifecycle sidecar $sidecar (errno=$errno)")
    }
    if (fd >= 0 && flock(fd, LOCK_EX or LOCK_NB) != 0) {
        close(fd)
        AppleLifecycleOwners.mutex.withLock { AppleLifecycleOwners.identities.remove(identity) }
        throw SqliteNowLifecycleBusyException(identity)
    }
    return appleLifecycleLease(identity, fd, registered = true)
}

private fun String.isAppleInMemoryPath(): Boolean =
    isEmpty() || this == ":memory:" || startsWith(":memory:") || startsWith(":temp:")

@OptIn(ExperimentalForeignApi::class)
private fun appleLifecycleLease(
    identity: String,
    fd: Int,
    registered: Boolean,
): SqliteNowLifecycleLease =
    object : SqliteNowLifecycleLease {
        override val identity: String = identity
        private var closed = false
        private var closing = false

        override suspend fun close() {
            AppleLifecycleOwners.mutex.withLock {
                if (closed || closing) return
                closing = true
            }
            var closeErr = 0
            if (fd >= 0 && close(fd) != 0) closeErr = errno
            if (closeErr != 0) {
                AppleLifecycleOwners.mutex.withLock { closing = false }
                throw SqliteNowException("Cannot close SQLiteNow lifecycle sidecar for $identity (errno=$closeErr)")
            }
            AppleLifecycleOwners.mutex.withLock {
                if (registered) AppleLifecycleOwners.identities.remove(identity)
                closed = true
                closing = false
            }
        }
    }
