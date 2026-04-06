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

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode

internal class OversqliteApplyExecutor(
    private val db: SafeSQLiteConnection,
    private val applyStateStore: OversqliteApplyStateStore,
) {
    suspend fun <T> inApplyModeTransaction(
        state: RuntimeState,
        block: suspend (StatementCache) -> T,
    ): T {
        return db.transaction(TransactionMode.IMMEDIATE) {
            db.execSQL("PRAGMA defer_foreign_keys = ON")
            val statementCache = StatementCache(db)
            try {
                applyStateStore.setApplyMode(true, statementCache)
                try {
                    block(statementCache)
                } finally {
                    applyStateStore.setApplyMode(false, statementCache)
                }
            } finally {
                statementCache.close()
            }
        }
    }
}
