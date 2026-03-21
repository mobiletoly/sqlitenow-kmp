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
package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.sqliteNetworkDispatcher
import kotlinx.coroutines.CoroutineDispatcher

/**
 * Bundle-era KMP oversqlite client.
 *
 * Construction is side-effect free. Call [bootstrap] successfully before running any sync
 * operation. Create at most one client instance per local database at a time; runtime sync
 * serialization is enforced per client instance, not across multiple client objects that point at
 * the same database.
 */
interface OversqliteClient {
    suspend fun pauseUploads()
    suspend fun resumeUploads()
    suspend fun pauseDownloads()
    suspend fun resumeDownloads()

    /** Validates local sync schema, creates metadata, binds user/source identity, and installs triggers. */
    suspend fun bootstrap(userId: String, sourceId: String): Result<Unit>

    /** Freezes local dirty rows into one logical bundle and uploads it through chunked push sessions. */
    suspend fun pushPending(): Result<Unit>

    /** Pulls authoritative remote bundles until the current stable bundle sequence is fully applied. */
    suspend fun pullToStable(): Result<Unit>

    /** Runs the standard interactive sync flow: push first, then pull. */
    suspend fun sync(): Result<Unit>

    /** Rebuilds local managed tables from a full staged server snapshot. */
    suspend fun hydrate(): Result<Unit>

    /** Rebuilds from snapshot and rotates local source identity as part of recovery. */
    suspend fun recover(): Result<Unit>

    /** Returns the last remote bundle sequence durably applied locally. */
    suspend fun lastBundleSeqSeen(): Result<Long>

    fun close()
}

open class PlatformDispatchers {
    open val io: CoroutineDispatcher = sqliteNetworkDispatcher()
}
