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

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/** Marks cross-module SQLiteNow runtime integration that is not an application-facing API. */
@RequiresOptIn(
    level = RequiresOptIn.Level.ERROR,
    message = "Use the generated Oversqlite coordinated-opening facade instead.",
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY)
annotation class SqliteNowInternalApi

internal class SqliteNowUnpublishedSessionToken

@SqliteNowInternalApi
class SqliteNowCoordinatedMigrationCloseContext :
    AbstractCoroutineContextElement(SqliteNowCoordinatedMigrationCloseContext) {
    companion object Key : CoroutineContext.Key<SqliteNowCoordinatedMigrationCloseContext>
}

@SqliteNowInternalApi
suspend fun requireSqliteNowCoordinatedCloseAllowed() {
    if (coroutineContext[SqliteNowCoordinatedMigrationCloseContext] != null) {
        throw SqliteNowCoordinatedMigrationException(
            diagnosticCode = "coordinated_migration_reentrant_close_forbidden",
            message = "Cannot close a coordinated database, opening, or client from its active migration context",
        )
    }
}

/**
 * Token-checked Core session used by Oversqlite while a database is not yet application-visible.
 *
 * The session never makes [SqliteNowDatabase.connection] usable. Publication is a separate final
 * operation after the coordinator has completed admission, migration, validation and client
 * binding. All session operations serialize so close joins the active operation before releasing
 * the reservation.
 */
@SqliteNowInternalApi
class SqliteNowUnpublishedSession internal constructor(
    private val database: SqliteNowDatabase,
    private val token: SqliteNowUnpublishedSessionToken,
    val migrationTargetVersion: Int?,
) {
    private val operationMutex = Mutex()
    private var acquired = false
    private var migrated = false
    private var published = false
    private var closed = false

    /** Acquires the lifecycle lease and provider inspection session without writable preparation. */
    suspend fun acquire() = operationMutex.withLock {
        requireActive()
        check(!acquired) { "Unpublished connection already acquired" }
        database.acquireUnpublishedConnection(token)
        acquired = true
    }

    /** True when provider preparation is required before complete local inspection. */
    val preparationRequired: Boolean
        get() {
            check(acquired) { "Unpublished provider session has not been acquired" }
            return database.unpublishedPreparationRequired(token)
        }

    /** True when the acquisition produced a side-effect-free inspection connection. */
    val inspectionAvailable: Boolean
        get() {
            check(acquired) { "Unpublished provider session has not been acquired" }
            return database.unpublishedInspectionAvailable(token)
        }

    /** Performs provider preparation after explicit coordinated-opening advancement. */
    suspend fun prepare() = operationMutex.withLock {
        requireActive()
        check(acquired) { "Unpublished provider session has not been acquired" }
        database.prepareUnpublishedConnection(token)
    }

    /** Runs a bounded integration operation against the still-unpublished connection. */
    suspend fun <T> withConnection(block: suspend (SafeSQLiteConnection) -> T): T =
        operationMutex.withLock {
            requireActive()
            check(acquired && inspectionAvailable) {
                "Unpublished connection is unavailable until provider preparation"
            }
            database.withUnpublishedConnection(token, block)
        }

    /**
     * Runs Core migrations and final validation in one transaction without publishing the result.
     * Admission must have completed through [withConnection] before this method is called.
     */
    suspend fun migrateAndValidate(
        fresh: Boolean = false,
        migrationSqlPolicy: SqliteNowCoordinatedMigrationSqlPolicy? = null,
        preInit: suspend (SafeSQLiteConnection) -> Unit = {},
        transactionSetup: suspend (SafeSQLiteConnection) -> Unit = {},
        validateBeforeCommit: suspend (SafeSQLiteConnection) -> Unit = {},
    ) = withContext(SqliteNowCoordinatedMigrationCloseContext()) {
        operationMutex.withLock {
            requireActive()
            check(acquired) { "Unpublished connection has not been acquired" }
            check(!migrated) { "Unpublished migration already completed" }
            database.migrateUnpublishedConnection(
                token,
                fresh,
                migrationSqlPolicy,
                preInit,
                transactionSetup,
                validateBeforeCommit,
            )
            migrated = true
        }
    }

    /** Binds the one coordinator-owned shutdown path before the connection becomes public. */
    suspend fun bindPublishedCloseHandler(closeHandler: suspend () -> Unit) = operationMutex.withLock {
        requireActive()
        check(acquired) { "Unpublished connection has not been acquired" }
        database.bindPublishedCloseHandler(token, closeHandler)
    }

    /** Publishes the acquired connection after all coordinated readiness work has completed. */
    suspend fun publish() = operationMutex.withLock {
        requireActive()
        check(acquired) { "Unpublished connection has not been acquired" }
        database.publishUnpublishedConnection(token)
        published = true
    }

    /** Revokes application admission synchronously while retaining private cleanup ownership. */
    fun revokePublicAccess() = database.revokeSessionAccess(token)

    /** Closes an unpublished session, joining any active operation first. */
    suspend fun close() {
        requireSqliteNowCoordinatedCloseAllowed()
        revokePublicAccess()
        operationMutex.withLock {
        if (closed) return@withLock
        if (published) {
            database.closePublishedConnection(token)
        } else {
            database.closeUnpublishedConnection(token)
        }
        closed = true
        }
    }

    private fun requireActive() {
        check(!closed) { "Coordinated opening session is closed" }
        check(!published) { "Coordinated opening session is already published" }
    }
}
