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

internal class OversqliteManagedTableStore(
    private val db: SafeSQLiteConnection,
    private val reusableStatementCleanup: ReusableStatementCleanup = DefaultReusableStatementCleanup,
) {
    suspend fun registerManagedTables(validated: ValidatedConfig) {
        db.withExclusiveAccess {
            db.prepare(
                """
                INSERT INTO _sync_managed_tables(schema_name, table_name)
                VALUES(?, ?)
                ON CONFLICT(schema_name, table_name) DO NOTHING
                """.trimIndent(),
            ).use { statement ->
                var used = false
                for (table in validated.tables) {
                    if (used) {
                        reusableStatementCleanup(statement)
                    } else {
                        used = true
                    }
                    statement.bindText(1, validated.schema)
                    statement.bindText(2, table.tableName)
                    statement.step()
                }
            }
        }
    }

    suspend fun loadManagedTables(schemaName: String): List<String> {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT table_name
                FROM _sync_managed_tables
                WHERE schema_name = ?
                ORDER BY table_name
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, schemaName)
                buildList {
                    while (st.step()) {
                        add(st.getText(0).lowercase())
                    }
                }
            }
        }
    }
}
