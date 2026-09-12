/*
 * Copyright 2026 Toly Pochkin
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

import java.io.File
import kotlinx.coroutines.withContext

@OptIn(SqliteNowInternalApi::class)
internal actual suspend fun createBundledCoordinatedOpeningProviderSession(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
): SqliteNowCoordinatedProviderSession {
    val inspectable = !dbName.isTransientDatabaseName() && File(dbName).isFile
    val inspection = if (inspectable) openReadOnlyInspectionConnection(dbName, debug, config) else null
    return DefaultSqliteNowCoordinatedProviderSession(
        inspectionConnection = inspection,
        preparationRequired = inspection == null,
        prepare = { BundledSqliteConnectionProvider.openConnection(dbName, debug, config) },
    )
}

private suspend fun openReadOnlyInspectionConnection(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
): SafeSQLiteConnection {
    val executionContext = createSqliteConnectionExecutionContext("$dbName:inspection")
    try {
        val connection = withContext(executionContext.dispatcher) {
            openBundledSqliteDriverInspectionConnection(dbName)
        }
        return SafeSQLiteConnection(
            ref = connection,
            debug = debug,
            activeClientIdentity = sqliteNowActiveClientIdentity(dbName, connection),
            persistenceController = NoopPersistenceController(),
            executionContextHook = config.executionContextHook,
            executionContext = executionContext,
        )
    } catch (failure: Throwable) {
        runCatching { executionContext.close() }
        throw failure
    }
}

private fun String.isTransientDatabaseName(): Boolean =
    isEmpty() || this == ":memory:" || startsWith(":memory:") || startsWith(":temp:")
