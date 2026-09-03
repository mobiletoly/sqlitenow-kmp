package dev.goquick.sqlitenow.core

import kotlin.test.Test
import kotlin.test.assertFailsWith

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
}
