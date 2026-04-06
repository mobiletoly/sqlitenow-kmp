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
import dev.goquick.sqlitenow.core.sqlite.use

internal class OversqliteApplyStateStore(
    private val db: SafeSQLiteConnection,
) {
    suspend fun isApplyMode(): Boolean {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT apply_mode
                FROM _sync_apply_state
                WHERE singleton_key = 1
                """.trimIndent(),
            ).use { st ->
                check(st.step()) { "_sync_apply_state singleton row is missing" }
                st.getLong(0) == 1L
            }
        }
    }

    suspend fun setApplyMode(
        enabled: Boolean,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_apply_state
                SET apply_mode = ?
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, if (enabled) 1 else 0)
            st.step()
        }
    }

    suspend fun resetOnStartup(statementCache: StatementCache? = null) {
        setApplyMode(enabled = false, statementCache = statementCache)
    }
}
