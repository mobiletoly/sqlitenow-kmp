package dev.goquick.sqlitenow.core.sqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class SqliteTransactionStateTest {

    @Test
    fun successfulTransactionControlsTrackExplicitAndSavepointState() {
        val scenarios = listOf(
            StateScenario(
                name = "explicit-commit",
                statements = listOf("BEGIN IMMEDIATE;", "COMMIT TRANSACTION;"),
                expectedInTransaction = listOf(true, false),
            ),
            StateScenario(
                name = "explicit-end",
                statements = listOf("begin deferred transaction;", "END;"),
                expectedInTransaction = listOf(true, false),
            ),
            StateScenario(
                name = "standalone-savepoint",
                statements = listOf("SAVEPOINT work;", "RELEASE SAVEPOINT work;"),
                expectedInTransaction = listOf(true, false),
            ),
            StateScenario(
                name = "rollback-to-keeps-target",
                statements = listOf(
                    "SAVEPOINT outer;",
                    "SAVEPOINT inner;",
                    "ROLLBACK TRANSACTION TO SAVEPOINT outer;",
                    "RELEASE outer;",
                ),
                expectedInTransaction = listOf(true, true, true, false),
            ),
            StateScenario(
                name = "release-outer-discards-nested-savepoints",
                statements = listOf(
                    "BEGIN;",
                    "SAVEPOINT outer;",
                    "SAVEPOINT inner;",
                    "RELEASE SAVEPOINT outer;",
                    "COMMIT;",
                ),
                expectedInTransaction = listOf(true, true, true, true, false),
            ),
            StateScenario(
                name = "duplicate-savepoint-names-use-most-recent-match",
                statements = listOf(
                    "SAVEPOINT repeated;",
                    "SAVEPOINT repeated;",
                    "SAVEPOINT child;",
                    "ROLLBACK TO repeated;",
                    "RELEASE repeated;",
                    "RELEASE repeated;",
                ),
                expectedInTransaction = listOf(true, true, true, true, true, false),
            ),
            StateScenario(
                name = "quoted-savepoints-are-case-insensitive",
                statements = listOf(
                    "SAVEPOINT\"Outer Name\";",
                    "SAVEPOINT[Inner];",
                    "ROLLBACK TRANSACTION TO SAVEPOINT`INNER`;",
                    "RELEASE SAVEPOINT'inner';",
                    "RELEASE SAVEPOINT'outer name';",
                ),
                expectedInTransaction = listOf(true, true, true, true, false),
            ),
            StateScenario(
                name = "non-ascii-savepoint-names-remain-distinct",
                statements = listOf(
                    "SAVEPOINT \"Ä\";",
                    "SAVEPOINT \"ä\";",
                    "RELEASE \"Ä\";",
                ),
                expectedInTransaction = listOf(true, true, false),
            ),
            StateScenario(
                name = "full-rollback-clears-explicit-and-savepoint-state",
                statements = listOf("BEGIN;", "SAVEPOINT nested;", "ROLLBACK TRANSACTION;"),
                expectedInTransaction = listOf(true, true, false),
            ),
            StateScenario(
                name = "unrelated-or-incomplete-tokens-do-not-guess-transitions",
                statements = listOf(
                    "BEGINNER;",
                    "SAVEPOINT;",
                    "BEGIN;",
                    "SELECT 1;",
                    "ROLLBACK TO;",
                    "ROLLBACKISH;",
                    "COMMIT;",
                ),
                expectedInTransaction = listOf(false, false, true, true, true, true, false),
            ),
        )

        scenarios.forEach { scenario ->
            val state = SqliteTransactionState()
            scenario.statements.zip(scenario.expectedInTransaction).forEachIndexed { index, (sql, expected) ->
                state.observeSuccessfulStatement(sql)
                assertEquals(
                    expected,
                    state.inTransaction(),
                    "${scenario.name} statement ${index + 1}: $sql",
                )
            }
        }
    }

    @Test
    fun resetClearsAllTrackedState() {
        val state = SqliteTransactionState()
        state.observeSuccessfulStatement("BEGIN")
        state.observeSuccessfulStatement("SAVEPOINT nested")

        state.reset()

        assertFalse(state.inTransaction())
    }

    private data class StateScenario(
        val name: String,
        val statements: List<String>,
        val expectedInTransaction: List<Boolean>,
    )
}
