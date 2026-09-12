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
import platform.linux.flock
import platform.posix.LOCK_EX
import platform.posix.LOCK_NB
import platform.posix.O_CREAT
import platform.posix.O_RDWR
import platform.posix.close
import platform.posix.errno
import platform.posix.open

private object LinuxLifecycleOwners {
    val mutex = Mutex()
    val identities = mutableSetOf<String>()
}

internal actual fun sqliteNowActiveClientIdentity(dbName: String, connection: SQLiteConnection): Any =
    if (dbName.isLinuxInMemoryPath()) Any() else canonicalSqliteNowDatabasePath(dbName)

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun acquireSqliteNowLifecycleLease(dbName: String): SqliteNowLifecycleLease {
    val inMemory = dbName.isLinuxInMemoryPath()
    if (inMemory) {
        return linuxLifecycleLease("memory:$dbName", -1, registered = false)
    }
    val identity = canonicalSqliteNowDatabasePath(dbName)
    LinuxLifecycleOwners.mutex.withLock {
        if (!LinuxLifecycleOwners.identities.add(identity)) {
            throw SqliteNowLifecycleBusyException(identity)
        }
    }
    val sidecar = "$identity.sqlitenow-opening-v1.lock"
    val fd = open(sidecar, O_CREAT or O_RDWR, 0x180)
    if (fd < 0) {
        LinuxLifecycleOwners.mutex.withLock { LinuxLifecycleOwners.identities.remove(identity) }
        throw SqliteNowException("Cannot open SQLiteNow lifecycle sidecar $sidecar (errno=$errno)")
    }
    if (flock(fd, LOCK_EX or LOCK_NB) != 0) {
        close(fd)
        LinuxLifecycleOwners.mutex.withLock { LinuxLifecycleOwners.identities.remove(identity) }
        throw SqliteNowLifecycleBusyException(identity)
    }
    return linuxLifecycleLease(identity, fd, registered = true)
}

private fun String.isLinuxInMemoryPath(): Boolean =
    isEmpty() || this == ":memory:" || startsWith(":memory:") || startsWith(":temp:")

@OptIn(ExperimentalForeignApi::class)
private fun linuxLifecycleLease(
    identity: String,
    fd: Int,
    registered: Boolean,
): SqliteNowLifecycleLease =
    object : SqliteNowLifecycleLease {
        override val identity: String = identity
        private var closed = false
        private var closing = false

        override suspend fun close() {
            LinuxLifecycleOwners.mutex.withLock {
                if (closed || closing) return
                closing = true
            }
            var closeErr = 0
            if (fd >= 0 && close(fd) != 0) closeErr = errno
            if (closeErr != 0) {
                LinuxLifecycleOwners.mutex.withLock { closing = false }
                throw SqliteNowException("Cannot close SQLiteNow lifecycle sidecar for $identity (errno=$closeErr)")
            }
            LinuxLifecycleOwners.mutex.withLock {
                if (registered) LinuxLifecycleOwners.identities.remove(identity)
                closed = true
                closing = false
            }
        }
    }
