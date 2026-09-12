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

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.sqlite.use

/** Application code invoked after one numbered migration boundary has been reached. */
typealias SqliteNowMigrationStepCallback = suspend (SqliteNowMigrationScope) -> Unit

/**
 * The version boundary currently being migrated.
 *
 * [connection] is deliberately narrower than [SafeSQLiteConnection]. It cannot close the
 * connection or begin, commit, or roll back a transaction, and it rejects lifecycle-owned SQL.
 */
class SqliteNowMigrationScope internal constructor(
    rawConnection: SafeSQLiteConnection,
    private val ownerAccess: MigrationOwnerAccess,
    val originalVersion: Int,
    val fromVersion: Int,
    val toVersion: Int,
    val targetVersion: Int,
) {
    val connection: SqliteNowMigrationConnection = SqliteNowMigrationConnection(rawConnection, ownerAccess)

    internal fun expire() {
        ownerAccess.expire()
    }

    internal suspend fun drain(
        cancelOperations: Boolean,
        propagateOperationFailure: Boolean,
    ) {
        ownerAccess.expireAndDrain(cancelOperations, propagateOperationFailure)
    }
}

/** Runtime entry point used by generated migration implementations. */
object SqliteNowMigrationStepRunner {
    suspend fun run(
        rawConnection: SafeSQLiteConnection,
        originalVersion: Int,
        fromVersion: Int,
        toVersion: Int,
        targetVersion: Int,
        callback: SqliteNowMigrationStepCallback,
    ) {
        val ownerAccess = rawConnection.captureMigrationOwnerAccess()
        val scope = SqliteNowMigrationScope(
            rawConnection = rawConnection,
            ownerAccess = ownerAccess,
            originalVersion = originalVersion,
            fromVersion = fromVersion,
            toVersion = toVersion,
            targetVersion = targetVersion,
        )
        var callbackFailure: Throwable? = null
        try {
            callback(scope)
        } catch (failure: Throwable) {
            callbackFailure = failure
            throw failure
        } finally {
            scope.expire()
            scope.drain(
                cancelOperations = callbackFailure != null,
                propagateOperationFailure = callbackFailure == null,
            )
        }
    }
}

/** Low-level SQL capability available only for the duration of a migration-step callback. */
class SqliteNowMigrationConnection internal constructor(
    private val connection: SafeSQLiteConnection,
    private val ownerAccess: MigrationOwnerAccess,
) {
    suspend fun execSQL(sql: String) {
        requireMigrationSqlAllowed(sql)
        connection.withMigrationOwnerAccess(ownerAccess) {
            connection.execSQL(sql)
        }
    }

    suspend fun <T> usePrepared(
        sql: String,
        block: suspend (SQLiteStatement) -> T,
    ): T {
        requireMigrationSqlAllowed(sql)
        return connection.withMigrationOwnerAccess(ownerAccess) {
            connection.prepare(sql).use { statement -> block(statement) }
        }
    }
}

private val forbiddenMigrationStatementKeywords = setOf(
    "BEGIN",
    "COMMIT",
    "END",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
)

/** Failure raised when coordinated migration SQL would bypass opening invariants. */
@SqliteNowInternalApi
class SqliteNowCoordinatedMigrationException(
    val diagnosticCode: String,
    message: String,
) : IllegalArgumentException(message)

/**
 * Exact SQL restrictions installed only while a coordinated migration body is running.
 *
 * Ordinary SQLiteNow migrations do not install this policy. Oversqlite supplies the exact
 * coordinator-owned object names for the active opening, so application objects are never
 * rejected merely because they share a reserved-looking prefix.
 */
@SqliteNowInternalApi
class SqliteNowCoordinatedMigrationSqlPolicy(
    forbiddenObjectNames: Set<String>,
) {
    private val forbiddenObjects = forbiddenObjectNames.mapTo(linkedSetOf()) { it.lowercase() }

    internal fun requireAllowed(sql: String) {
        try {
            requireMigrationSqlAllowed(sql)
        } catch (failure: IllegalArgumentException) {
            throw SqliteNowCoordinatedMigrationException(
                diagnosticCode = "coordinated_migration_lifecycle_sql_forbidden",
                message = failure.message ?: "lifecycle SQL is forbidden during coordinated migration",
            )
        }

        val sanitized = stripMigrationSqlCommentsAndStrings(sql)
        for ((statementStart, statementEnd) in migrationSqlStatementRanges(sanitized)) {
            val statement = sanitized.substring(statementStart, statementEnd)
            val originalStatement = sql.substring(statementStart, statementEnd)
            val keywordMatch = Regex("[A-Za-z_]+").find(statement) ?: continue
            val keyword = keywordMatch.value.uppercase()
            if (keyword == "ATTACH" || keyword == "DETACH") {
                fail("coordinated_migration_attachment_sql_forbidden", "$keyword is forbidden during coordinated migration")
            }
            if (keyword == "PRAGMA") {
                rejectIntegrityDisablingPragma(originalStatement, keywordMatch.range.last + 1)
            }
        }

        val protected = migrationSqlIdentifiers(sql).firstOrNull { it.lowercase() in forbiddenObjects }
        if (protected != null) {
            fail(
                "coordinated_migration_protected_object_forbidden",
                "direct access to coordinator-owned object $protected is forbidden during coordinated migration",
            )
        }
    }

    private fun rejectIntegrityDisablingPragma(statement: String, startIndex: Int) {
        val pragma = readMigrationPragma(statement, startIndex) ?: return
        val name = pragma.name.lowercase()
        if (pragma.hasAssignment && !pragma.valid && name in integritySensitiveMigrationPragmas) {
            fail(
                "coordinated_migration_integrity_pragma_forbidden",
                "PRAGMA $name has a malformed integrity-sensitive assignment",
            )
        }
        val value = pragma.value?.lowercase() ?: return
        val booleanValue = migrationPragmaBoolean(value)
        val disablesIntegrity = when (name) {
            "foreign_keys" -> booleanValue != MigrationPragmaBoolean.ENABLED
            "defer_foreign_keys", "ignore_check_constraints", "writable_schema", "trusted_schema" ->
                booleanValue != MigrationPragmaBoolean.DISABLED
            "journal_mode" -> value !in setOf("delete", "truncate", "persist", "wal")
            "locking_mode" -> true
            else -> false
        }
        if (disablesIntegrity) {
            fail(
                "coordinated_migration_integrity_pragma_forbidden",
                "PRAGMA $name=$value may not disable integrity during coordinated migration",
            )
        }
    }

    private fun fail(code: String, message: String): Nothing =
        throw SqliteNowCoordinatedMigrationException(code, message)
}

internal fun requireMigrationSqlAllowed(sql: String) {
    val sanitized = stripMigrationSqlCommentsAndStrings(sql)
    for ((statementStart, statementEnd) in migrationSqlStatementRanges(sanitized)) {
        val sanitizedStatement = sanitized.substring(statementStart, statementEnd)
        val originalStatement = sql.substring(statementStart, statementEnd)
        val keywordMatch = Regex("[A-Za-z_]+").find(sanitizedStatement) ?: continue
        val keyword = keywordMatch.value.uppercase()
        require(keyword !in forbiddenMigrationStatementKeywords) {
            "$keyword is not allowed from a migration-step callback"
        }

        if (keyword == "PRAGMA") {
            val pragmaName = readMigrationPragma(
                originalStatement,
                keywordMatch.range.last + 1,
            )?.name
            require(!pragmaName.equals("user_version", ignoreCase = true)) {
                "PRAGMA user_version is owned by SQLiteNow during migration"
            }
        }
    }
}

private fun migrationSqlStatementRanges(sql: String): List<Pair<Int, Int>> {
    val ranges = mutableListOf<Pair<Int, Int>>()
    var statementStart = 0
    var index = 0
    var trigger = false
    var triggerBody = false
    var triggerEndSeen = false
    var caseDepth = 0
    val leadingWords = mutableListOf<String>()

    fun resetStatement() {
        trigger = false
        triggerBody = false
        triggerEndSeen = false
        caseDepth = 0
        leadingWords.clear()
    }

    while (index < sql.length) {
        if (sql[index].isLetter() || sql[index] == '_') {
            val wordStart = index
            index++
            while (index < sql.length && (sql[index].isLetterOrDigit() || sql[index] == '_')) index++
            val word = sql.substring(wordStart, index).uppercase()
            if (!triggerBody) {
                if (leadingWords.size < 4) leadingWords += word
                trigger = trigger || leadingWords == listOf("CREATE", "TRIGGER") ||
                    leadingWords == listOf("CREATE", "TEMP", "TRIGGER") ||
                    leadingWords == listOf("CREATE", "TEMPORARY", "TRIGGER")
                if (trigger && word == "BEGIN") triggerBody = true
            } else {
                when (word) {
                    "CASE" -> caseDepth++
                    "END" -> {
                        if (caseDepth > 0) caseDepth-- else triggerEndSeen = true
                    }
                }
            }
            continue
        }

        if (sql[index] == ';' && (!triggerBody || triggerEndSeen)) {
            ranges += statementStart to index
            statementStart = index + 1
            resetStatement()
        }
        index++
    }
    if (statementStart < sql.length) ranges += statementStart to sql.length
    return ranges
}

private data class MigrationPragma(
    val name: String,
    val value: String?,
    val hasAssignment: Boolean,
    val valid: Boolean,
)

private data class MigrationPragmaValue(val value: String, val endIndex: Int)

private val integritySensitiveMigrationPragmas = setOf(
    "foreign_keys",
    "defer_foreign_keys",
    "ignore_check_constraints",
    "writable_schema",
    "trusted_schema",
    "journal_mode",
    "locking_mode",
)

private fun readMigrationPragma(statement: String, startIndex: Int): MigrationPragma? {
    var index = skipMigrationSqlTrivia(statement, startIndex) ?: return null
    val first = readMigrationSqlIdentifier(statement, index) ?: return null
    index = skipMigrationSqlTrivia(statement, first.second) ?: return null
    var name = first.first
    if (statement.getOrNull(index) == '.') {
        index = skipMigrationSqlTrivia(statement, index + 1) ?: return null
        val second = readMigrationSqlIdentifier(statement, index) ?: return null
        name = second.first
        index = skipMigrationSqlTrivia(statement, second.second) ?: return null
    }
    val operator = statement.getOrNull(index)
    if (operator != '=' && operator != '(') return MigrationPragma(name, null, false, valid = true)
    val parenthesized = operator == '('
    if (operator == '=' && statement.getOrNull(index + 1) == '=') index += 2 else index++
    index = skipMigrationSqlTrivia(statement, index)
        ?: return MigrationPragma(name, null, true, valid = false)
    val parsedValue = readMigrationPragmaValue(statement, index)
        ?: return MigrationPragma(name, null, true, valid = false)
    index = skipMigrationSqlTrivia(statement, parsedValue.endIndex)
        ?: return MigrationPragma(name, parsedValue.value, true, valid = false)
    if (parenthesized) {
        if (statement.getOrNull(index) != ')') {
            return MigrationPragma(name, parsedValue.value, true, valid = false)
        }
        index = skipMigrationSqlTrivia(statement, index + 1)
            ?: return MigrationPragma(name, parsedValue.value, true, valid = false)
    }
    return MigrationPragma(name, parsedValue.value, true, valid = index == statement.length)
}

private enum class MigrationPragmaBoolean { ENABLED, DISABLED, UNKNOWN }

private fun migrationPragmaBoolean(value: String): MigrationPragmaBoolean {
    val normalized = value.lowercase()
    if (normalized in setOf("on", "true", "yes")) return MigrationPragmaBoolean.ENABLED
    if (normalized in setOf("off", "false", "no")) return MigrationPragmaBoolean.DISABLED
    val unsigned = normalized.removePrefix("+").removePrefix("-")
    val hexadecimal = unsigned.startsWith("0x")
    val digits = if (hexadecimal) unsigned.drop(2) else unsigned
    val radix = if (hexadecimal) 16 else 10
    if (digits.isEmpty() || digits.any { it.digitToIntOrNull(radix) == null }) {
        return MigrationPragmaBoolean.UNKNOWN
    }
    return if (digits.all { it == '0' }) MigrationPragmaBoolean.DISABLED else MigrationPragmaBoolean.ENABLED
}

private fun readMigrationPragmaValue(sql: String, startIndex: Int): MigrationPragmaValue? {
    var index = startIndex
    if (sql.getOrNull(index) == '+' || sql.getOrNull(index) == '-') index++
    val hexadecimal = sql.getOrNull(index) == '0' && sql.getOrNull(index + 1)?.lowercaseChar() == 'x'
    if (hexadecimal) index += 2
    val digitsStart = index
    val radix = if (hexadecimal) 16 else 10
    while (index < sql.length && sql[index].digitToIntOrNull(radix) != null) index++
    if (index > digitsStart) return MigrationPragmaValue(sql.substring(startIndex, index), index)
    return readMigrationSqlIdentifier(sql, startIndex)?.let { MigrationPragmaValue(it.first, it.second) }
}

private fun skipMigrationSqlTrivia(sql: String, startIndex: Int): Int? {
    var index = startIndex
    while (index < sql.length) {
        when {
            sql[index].isWhitespace() -> index++
            sql[index] == '-' && sql.getOrNull(index + 1) == '-' -> {
                index += 2
                while (index < sql.length && sql[index] != '\n') index++
            }
            sql[index] == '/' && sql.getOrNull(index + 1) == '*' -> {
                index += 2
                var terminated = false
                while (index < sql.length) {
                    if (sql[index] == '*' && sql.getOrNull(index + 1) == '/') {
                        index += 2
                        terminated = true
                        break
                    }
                    index++
                }
                if (!terminated) return null
            }
            else -> return index
        }
    }
    return index
}

private fun readMigrationSqlIdentifier(sql: String, startIndex: Int): Pair<String, Int>? {
    if (startIndex >= sql.length) return null
    val opening = sql[startIndex]
    if (opening == '\'' || opening == '"' || opening == '`' || opening == '[') {
        val closing = if (opening == '[') ']' else opening
        val value = StringBuilder()
        var index = startIndex + 1
        while (index < sql.length) {
            if (sql[index] == closing) {
                if (sql.getOrNull(index + 1) == closing) {
                    value.append(closing)
                    index += 2
                    continue
                }
                return value.toString() to (index + 1)
            }
            value.append(sql[index])
            index++
        }
        return null
    }

    var index = startIndex
    while (index < sql.length && (sql[index].isLetterOrDigit() || sql[index] == '_')) index++
    if (index == startIndex) return null
    return sql.substring(startIndex, index) to index
}

private fun migrationSqlIdentifiers(sql: String): List<String> {
    val identifiers = mutableListOf<String>()
    var index = 0
    while (index < sql.length) {
        when {
            sql[index] == '\'' -> {
                index++
                while (index < sql.length) {
                    if (sql[index] == '\'' && sql.getOrNull(index + 1) == '\'') {
                        index += 2
                    } else if (sql[index] == '\'') {
                        index++
                        break
                    } else {
                        index++
                    }
                }
            }
            sql[index] == '-' && sql.getOrNull(index + 1) == '-' -> {
                index += 2
                while (index < sql.length && sql[index] != '\n') index++
            }
            sql[index] == '/' && sql.getOrNull(index + 1) == '*' -> {
                index += 2
                while (index < sql.length && !(sql[index] == '*' && sql.getOrNull(index + 1) == '/')) index++
                index = (index + 2).coerceAtMost(sql.length)
            }
            sql[index].isLetter() || sql[index] == '_' || sql[index] in setOf('"', '`', '[') -> {
                val identifier = readMigrationSqlIdentifier(sql, index)
                if (identifier == null) {
                    index++
                } else {
                    identifiers += identifier.first
                    index = identifier.second
                }
            }
            else -> index++
        }
    }
    return identifiers
}

private fun stripMigrationSqlCommentsAndStrings(sql: String): String {
    val result = StringBuilder(sql.length)
    var index = 0
    while (index < sql.length) {
        when {
            sql[index] == '\'' || sql[index] == '"' || sql[index] == '`' || sql[index] == '[' -> {
                val opening = sql[index]
                val closing = if (opening == '[') ']' else opening
                result.append(' ')
                index++
                while (index < sql.length) {
                    if (sql[index] == closing && sql.getOrNull(index + 1) == closing) {
                        result.append("  ")
                        index += 2
                    } else if (sql[index] == closing) {
                        result.append(' ')
                        index++
                        break
                    } else {
                        result.append(if (sql[index] == '\n') '\n' else ' ')
                        index++
                    }
                }
            }
            sql[index] == '-' && sql.getOrNull(index + 1) == '-' -> {
                result.append("  ")
                index += 2
                while (index < sql.length && sql[index] != '\n') {
                    result.append(' ')
                    index++
                }
            }
            sql[index] == '/' && sql.getOrNull(index + 1) == '*' -> {
                result.append("  ")
                index += 2
                while (index < sql.length) {
                    if (sql[index] == '*' && sql.getOrNull(index + 1) == '/') {
                        result.append("  ")
                        index += 2
                        break
                    }
                    result.append(if (sql[index] == '\n') '\n' else ' ')
                    index++
                }
            }
            else -> {
                result.append(sql[index])
                index++
            }
        }
    }
    return result.toString()
}
