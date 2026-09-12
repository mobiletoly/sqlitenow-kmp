package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.sqlite.OwnedStatementClose
import dev.goquick.sqlitenow.core.sqlite.trackSQLiteStatement
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class StatementClosePlatformTest {
    @Test
    fun callerAndOwnerShareRetryProgressOnTheActualPlatformStatement() = runTest {
        for (failures in listOf(0, 1, 3)) {
            val connection = BundledSqliteConnectionProvider.openConnection(":memory:", false)
            val raw = connection.prepare("SELECT 1")
            var calls = 0
            var observedFailures = 0
            val statement = trackSQLiteStatement(
                object : SQLiteStatement by raw {
                    override fun close() {
                        calls++
                        if (calls <= failures) error("held statement cleanup")
                        raw.close()
                    }
                },
                cleanupFailureObserver = { observedFailures++ },
                beforeCloseObserver = {},
                closeSuccessObserver = {},
            )
            val owner = statement as OwnedStatementClose
            try {
                if (failures > 0) {
                    assertFailsWith<IllegalStateException> { statement.close() }
                    assertFalse(owner.closeProgress.physicallyComplete)
                    repeat(failures - 1) {
                        assertFailsWith<IllegalStateException> { owner.closeFromOwner() }
                        assertFalse(owner.closeProgress.physicallyComplete)
                    }
                }
                owner.closeFromOwner()
                assertTrue(owner.closeProgress.physicallyComplete)
                statement.close()
                owner.closeFromOwner()
                assertEquals(failures + 1, calls)
                assertEquals(failures, observedFailures)
            } finally {
                owner.closeFromOwner()
                connection.close()
            }
        }
    }

    @Test
    fun sameThreadCloseReentrancyRejectsBeforeDelegateReentry() = runTest {
        val connection = BundledSqliteConnectionProvider.openConnection(":memory:", false)
        val raw = connection.prepare("SELECT 1")
        lateinit var statement: SQLiteStatement
        var calls = 0
        statement = trackSQLiteStatement(
            object : SQLiteStatement by raw {
                override fun close() {
                    calls++
                    val error = assertFailsWith<IllegalStateException> { statement.close() }
                    assertTrue(error.message.orEmpty().contains("reenter statement close"))
                    raw.close()
                }
            }, cleanupFailureObserver = { throw AssertionError("unexpected cleanup failure", it) },
            beforeCloseObserver = {}, closeSuccessObserver = {},
        )
        statement.close()
        (statement as OwnedStatementClose).closeFromOwner()
        assertEquals(1, calls)
        connection.close()
    }
}
