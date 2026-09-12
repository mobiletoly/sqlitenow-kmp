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
import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

private object JvmAndroidLifecycleOwners {
    val identities = mutableSetOf<String>()
}

internal var jvmAndroidLifecycleLockReleaseForTest: ((FileLock) -> Unit)? = null
internal var jvmAndroidLifecycleCloseJoinedForTest: (() -> Unit)? = null
internal var jvmAndroidLifecycleFileCloseForTest: ((RandomAccessFile) -> Unit)? = null

internal actual fun sqliteNowActiveClientIdentity(dbName: String, connection: SQLiteConnection): Any =
    if (dbName.isJvmAndroidInMemoryPath()) Any() else canonicalJvmAndroidDatabasePath(dbName)

internal actual suspend fun acquireSqliteNowLifecycleLease(dbName: String): SqliteNowLifecycleLease {
    val inMemory = dbName.isJvmAndroidInMemoryPath()
    if (inMemory) {
        return object : SqliteNowLifecycleLease {
            override val identity: String = "memory:$dbName"
            override suspend fun close() = Unit
        }
    }
    val identity = canonicalJvmAndroidDatabasePath(dbName)

    synchronized(JvmAndroidLifecycleOwners) {
        if (!JvmAndroidLifecycleOwners.identities.add(identity)) {
            throw SqliteNowLifecycleBusyException(identity)
        }
    }

    var file: RandomAccessFile? = null
    var lock: FileLock? = null
    try {
        val sidecar = File("$identity.sqlitenow-opening-v1.lock")
        file = RandomAccessFile(sidecar, "rw")
        lock = try {
            file.channel.tryLock()
        } catch (_: OverlappingFileLockException) {
            null
        }
        if (lock == null) {
            throw SqliteNowLifecycleBusyException(identity)
        }
    } catch (t: Throwable) {
        runCatching { lock?.release() }
        runCatching { file?.close() }
        synchronized(JvmAndroidLifecycleOwners) {
            JvmAndroidLifecycleOwners.identities.remove(identity)
        }
        throw t
    }

    return object : SqliteNowLifecycleLease {
        override val identity: String = identity
        private val closeMonitor = Any()
        private var attempt: CompletableDeferred<Throwable?>? = null
        private var attemptThread: Thread? = null
        private var lockReleased = false
        private var fileClosed = false

        override suspend fun close() {
            val outcome = withContext(NonCancellable) {
                val (selected, owner) = synchronized(closeMonitor) {
                    attempt?.let {
                        check(attemptThread !== Thread.currentThread()) {
                            "Cannot reenter lifecycle lease close from its cleanup owner"
                        }
                        return@synchronized it to false
                    }
                    if (fileClosed) return@withContext null
                    CompletableDeferred<Throwable?>().also {
                        attempt = it
                        attemptThread = Thread.currentThread()
                    } to true
                }
                if (!owner) {
                    jvmAndroidLifecycleCloseJoinedForTest?.invoke()
                    return@withContext selected.await()
                }
                // OS calls and test hooks run outside the registry and attempt monitors.
                var failure: Throwable? = null
                if (!lockReleased) {
                    try {
                        jvmAndroidLifecycleLockReleaseForTest?.invoke(lock) ?: lock.release()
                        lockReleased = true
                    } catch (t: Throwable) {
                        failure = t
                    }
                }
                if (!fileClosed) {
                    try {
                        jvmAndroidLifecycleFileCloseForTest?.invoke(file) ?: file.close()
                        fileClosed = true
                    } catch (t: Throwable) {
                        failure = combineLifecycleFailures(failure, t)
                    }
                }
                if (fileClosed) {
                    // Closing the descriptor also ends an unreleased file lock.
                    lockReleased = true
                    synchronized(JvmAndroidLifecycleOwners) {
                        JvmAndroidLifecycleOwners.identities.remove(identity)
                    }
                }
                selected.complete(failure)
                synchronized(closeMonitor) {
                    attempt = null
                    attemptThread = null
                }
                failure
            }
            outcome?.let { throw it }
        }
    }
}

private fun canonicalJvmAndroidDatabasePath(dbName: String): String {
    val databaseFile = File(dbName).absoluteFile
    if (!databaseFile.exists() && Files.isSymbolicLink(databaseFile.toPath())) {
        throw SqliteNowException("SQLiteNow database path is a dangling symbolic link: ${databaseFile.path}")
    }
    return if (databaseFile.exists()) {
        databaseFile.canonicalFile.path
    } else {
        val canonicalParent = databaseFile.parentFile?.canonicalFile
            ?: throw SqliteNowException("SQLiteNow database path has no parent: ${databaseFile.path}")
        File(canonicalParent, databaseFile.name).path
    }
}

private fun String.isJvmAndroidInMemoryPath(): Boolean =
    isEmpty() || this == ":memory:" || startsWith(":memory:") || startsWith(":temp:")

private fun combineLifecycleFailures(primary: Throwable?, later: Throwable): Throwable {
    if (primary == null) return later
    primary.addSuppressed(later)
    return primary
}
