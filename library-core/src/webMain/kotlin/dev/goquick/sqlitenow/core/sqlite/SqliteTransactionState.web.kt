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
package dev.goquick.sqlitenow.core.sqlite

internal class SqliteTransactionState {
    private var transactionDepth = 0

    fun observeExecSql(sql: String) {
        val normalized = sql.trim()
        when {
            normalized.startsWith("BEGIN", ignoreCase = true) -> transactionDepth++
            normalized.startsWith("COMMIT", ignoreCase = true) -> if (transactionDepth > 0) transactionDepth--
            normalized.startsWith("ROLLBACK", ignoreCase = true) -> if (transactionDepth > 0) transactionDepth--
        }
    }

    fun inTransaction(): Boolean = transactionDepth > 0

    fun reset() {
        transactionDepth = 0
    }
}
