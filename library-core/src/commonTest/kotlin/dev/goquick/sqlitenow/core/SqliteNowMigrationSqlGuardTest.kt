package dev.goquick.sqlitenow.core

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals

@OptIn(SqliteNowInternalApi::class)
class SqliteNowMigrationSqlGuardTest {
    @Test
    fun rejectsTransactionControlAndUserVersionStatements() {
        val forbidden = listOf(
            "BEGIN",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT nested",
            "RELEASE nested",
            "PRAGMA user_version",
            "PRAGMA main.user_version = 99",
            "PRAGMA 'user_version' = 99",
            "PRAGMA \"main\".'user_version'(99)",
            "-- comment\nPRAGMA [user_version](99)",
        )

        forbidden.forEach { sql ->
            assertFailsWith<IllegalArgumentException>(sql) {
                requireMigrationSqlAllowed(sql)
            }
        }
    }

    @Test
    fun acceptsOrdinaryExecuteQueryAndPreparedStatementSql() {
        listOf(
            "UPDATE person SET name = 'COMMIT' WHERE id = 1",
            "UPDATE \"orders; END\" SET value = 1",
            "UPDATE `orders; COMMIT` SET value = 1",
            "UPDATE [orders; ROLLBACK] SET value = 1",
            """
                CREATE TRIGGER person_audit AFTER UPDATE ON person BEGIN
                    INSERT INTO audit(message) VALUES ('updated');
                END;
            """.trimIndent(),
            "SELECT user_version FROM application_metadata",
            "PRAGMA foreign_keys",
            "/* BEGIN */ INSERT INTO person(id, name) VALUES (1, 'Ada')",
        ).forEach(::requireMigrationSqlAllowed)
    }

    @Test
    fun rejectsTransactionControlAfterTriggerDefinition() {
        assertFailsWith<IllegalArgumentException> {
            requireMigrationSqlAllowed(
                """
                    CREATE TRIGGER person_audit AFTER UPDATE ON person BEGIN
                        INSERT INTO audit(message) VALUES ('updated');
                    END;
                    COMMIT;
                """.trimIndent()
            )
        }
    }

    @Test
    fun coordinatedPolicyRejectsLifecycleIntegrityAndExactProtectedObjects() {
        val policy = SqliteNowCoordinatedMigrationSqlPolicy(setOf("_sync_opening_state", "owned_guard"))
        val scenarios = listOf(
            "transaction" to "BEGIN IMMEDIATE",
            "savepoint" to "SAVEPOINT nested",
            "attach" to "ATTACH DATABASE 'other.db' AS other",
            "detach" to "DETACH DATABASE other",
            "user-version" to "PRAGMA user_version = 7",
            "foreign-keys" to "PRAGMA foreign_keys = OFF",
            "quoted-foreign-keys" to "PRAGMA \"foreign_keys\" = 'OFF'",
            "deferred-foreign-keys" to "PRAGMA defer_foreign_keys = ON",
            "check-constraints" to "PRAGMA ignore_check_constraints(1)",
            "writable-schema" to "PRAGMA writable_schema = TRUE",
            "trusted-schema" to "PRAGMA trusted_schema = ON",
            "journal-mode" to "PRAGMA journal_mode = OFF",
            "locking-mode" to "PRAGMA locking_mode = EXCLUSIVE",
            "protected-table" to "DELETE FROM _sync_opening_state",
            "quoted-protected-table" to "UPDATE \"_sync_opening_state\" SET phase='ready'",
            "protected-trigger-body" to """
                CREATE TRIGGER application_trigger AFTER INSERT ON notes BEGIN
                    DELETE FROM _sync_opening_state;
                END;
            """.trimIndent(),
            "protected-guard" to "DROP TRIGGER [owned_guard]",
        )

        scenarios.forEach { (name, sql) ->
            val failure = assertFailsWith<SqliteNowCoordinatedMigrationException>(name) {
                policy.validateForTest(sql)
            }
            assertEquals(true, failure.diagnosticCode.startsWith("coordinated_migration_"), name)
        }
    }

    @Test
    fun coordinatedPolicyRejectsEveryUnsafeOrUnknownIntegrityPragmaValue() {
        val policy = SqliteNowCoordinatedMigrationSqlPolicy(emptySet())
        val scenarios = listOf(
            "foreign-keys-zero" to "PRAGMA foreign_keys = 0",
            "foreign-keys-double-equals-zero" to "PRAGMA foreign_keys == 0",
            "foreign-keys-unknown" to "PRAGMA foreign_keys = maybe",
            "foreign-keys-double-equals-unknown" to "PRAGMA foreign_keys == maybe",
            "foreign-keys-missing-value" to "PRAGMA foreign_keys =",
            "foreign-keys-double-equals-missing-value" to "PRAGMA foreign_keys ==",
            "deferred-signed-hex" to "PRAGMA defer_foreign_keys = +0x1",
            "checks-negative-hex" to "PRAGMA ignore_check_constraints(-0x1)",
            "foreign-keys-incomplete-hex" to "PRAGMA foreign_keys = 0x",
            "foreign-keys-suffixed-decimal" to "PRAGMA foreign_keys = 0abc",
            "foreign-keys-suffixed-hex" to "PRAGMA foreign_keys = 0x0g",
            "foreign-keys-unterminated-single-quote" to "PRAGMA foreign_keys = 'ON",
            "foreign-keys-unterminated-double-quote" to "PRAGMA foreign_keys = \"ON",
            "foreign-keys-unterminated-backtick" to "PRAGMA foreign_keys = `ON",
            "foreign-keys-unterminated-bracket" to "PRAGMA foreign_keys = [ON",
            "foreign-keys-missing-closing-parenthesis" to "PRAGMA foreign_keys(ON",
            "foreign-keys-trailing-token" to "PRAGMA foreign_keys = ON suffix",
            "foreign-keys-unterminated-comment" to "PRAGMA foreign_keys = ON /*",
            "deferred-positive" to "PRAGMA defer_foreign_keys = 2",
            "checks-negative" to "PRAGMA ignore_check_constraints = -1",
            "writable-positive" to "PRAGMA writable_schema(+2)",
            "trusted-unknown" to "PRAGMA trusted_schema = maybe",
            "journal-memory" to "PRAGMA journal_mode = MEMORY",
            "journal-unknown" to "PRAGMA journal_mode = invented",
        )

        scenarios.forEach { (name, sql) ->
            assertFailsWith<SqliteNowCoordinatedMigrationException>(name) {
                policy.validateForTest(sql)
            }
        }
    }

    @Test
    fun coordinatedPolicyAllowsOrdinaryMigrationSqlAndPrefixCollisions() {
        val policy = SqliteNowCoordinatedMigrationSqlPolicy(setOf("_sqlitenow_preserve_owned"))
        listOf(
            "CREATE TABLE notes(id TEXT PRIMARY KEY, value TEXT)",
            "INSERT INTO notes VALUES('one', 'BEGIN _sync_opening_state')",
            "PRAGMA foreign_keys = ON",
            "PRAGMA foreign_keys == ON",
            "PRAGMA /* assignment */ foreign_keys /* operator */ == /* value */ ON",
            "PRAGMA foreign_keys = -1",
            "PRAGMA foreign_keys = 0x1",
            "PRAGMA foreign_keys == +0X0001 /* enabled */",
            "PRAGMA defer_foreign_keys = OFF",
            "PRAGMA writable_schema = 0",
            "PRAGMA writable_schema(-0x0000)",
            "PRAGMA writable_schema = +000000000000000000000000000000000000000000",
            "PRAGMA journal_mode = WAL",
            "CREATE TRIGGER _sqlitenow_preserve_application AFTER INSERT ON notes BEGIN " +
                "UPDATE notes SET value='kept' WHERE id=NEW.id; END;",
        ).forEach(policy::validateForTest)
    }
}

@OptIn(SqliteNowInternalApi::class)
private fun SqliteNowCoordinatedMigrationSqlPolicy.validateForTest(sql: String) = requireAllowed(sql)
