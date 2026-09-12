/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import java.lang.management.ManagementFactory
import java.nio.file.Files
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

internal enum class JvmCleanupFailureMode {
    RESET_THEN_CLEAR,
    CLEAR_ONLY,
    CLOSE_ONCE,
}

internal class JvmStatementCleanupFault private constructor(
    private val mode: JvmCleanupFailureMode,
    originalConnection: SQLiteConnection,
    sqlMatcher: (String) -> Boolean,
) {
    val resetFailure = IllegalStateException("RAW_RESET_FAILURE")
    val clearFailure = IllegalStateException("RAW_CLEAR_FAILURE")
    val closeFailure = IllegalStateException("RAW_CLOSE_FAILURE")
    val postCleanupWorkFailure = IllegalStateException("statement work rejected after cleanup failure")
    private val connection = FaultingConnection(originalConnection, sqlMatcher)
    private var cleanupFailureObserved = false

    var allStatementBindOrStepCallsAfterFailure: Int = 0
        private set

    var differentStatementBindOrStepCallsAfterFailure: Int = 0
        private set

    val prepareCalls: Int
        get() = connection.prepareCalls

    val rawConnectionCloseCalls: Int
        get() = connection.closeCalls

    val selectedStatement: FaultingStatement
        get() = requireNotNull(connection.selectedStatement) { "the expected raw statement was not prepared" }

    fun assertFailureGraph(observed: Throwable) {
        val statement = selectedStatement
        if (mode == JvmCleanupFailureMode.CLOSE_ONCE) {
            assertEquals(listOf("close", "close"), statement.events)
            assertEquals(1, observed.countIdentity(closeFailure))
            assertEquals(0, observed.countIdentity(resetFailure))
            assertEquals(0, observed.countIdentity(clearFailure))
            assertTrue(closeFailure.suppressed.isEmpty())
            return
        }
        assertEquals(listOf("reset", "clear"), statement.events.take(2))
        assertEquals(1, statement.resetCalls)
        assertEquals(1, statement.clearCalls)
        when (mode) {
            JvmCleanupFailureMode.RESET_THEN_CLEAR -> {
                assertEquals(1, observed.countIdentity(resetFailure))
                assertEquals(1, observed.countIdentity(clearFailure))
                assertEquals(listOf(clearFailure), resetFailure.suppressed.toList())
            }
            JvmCleanupFailureMode.CLEAR_ONLY -> {
                assertEquals(0, observed.countIdentity(resetFailure))
                assertEquals(1, observed.countIdentity(clearFailure))
                assertTrue(clearFailure.suppressed.isEmpty())
            }
            JvmCleanupFailureMode.CLOSE_ONCE -> error("handled above")
        }
    }

    private inner class FaultingConnection(
        private val delegate: SQLiteConnection,
        private val sqlMatcher: (String) -> Boolean,
    ) : SQLiteConnection {
        var prepareCalls: Int = 0
            private set
        var closeCalls: Int = 0
            private set
        var selectedStatement: FaultingStatement? = null
            private set

        override fun prepare(sql: String): SQLiteStatement {
            prepareCalls++
            val statement = delegate.prepare(sql)
            val injectCleanupFailure = selectedStatement == null && sqlMatcher(sql)
            // Fatal transaction cleanup must still issue its rollback; reject any other later statement work.
            val observePostCleanupWork = !cleanupFailureObserved || sql != "ROLLBACK"
            return FaultingStatement(statement, injectCleanupFailure, observePostCleanupWork).also { wrapped ->
                if (injectCleanupFailure) selectedStatement = wrapped
            }
        }

        override fun inTransaction(): Boolean = delegate.inTransaction()

        override fun close() {
            closeCalls++
            delegate.close()
        }
    }

    internal inner class FaultingStatement(
        private val delegate: SQLiteStatement,
        private val injectCleanupFailure: Boolean,
        private val observePostCleanupWork: Boolean,
    ) : SQLiteStatement {
        val events = mutableListOf<String>()
        var resetCalls: Int = 0
            private set
        var clearCalls: Int = 0
            private set
        var closeCalls: Int = 0
            private set
        var bindOrStepCallsAfterFailure: Int = 0
            private set

        override fun bindBlob(index: Int, value: ByteArray) {
            observeBindOrStep()
            delegate.bindBlob(index, value)
        }

        override fun bindDouble(index: Int, value: Double) {
            observeBindOrStep()
            delegate.bindDouble(index, value)
        }

        override fun bindLong(index: Int, value: Long) {
            observeBindOrStep()
            delegate.bindLong(index, value)
        }

        override fun bindText(index: Int, value: String) {
            observeBindOrStep()
            delegate.bindText(index, value)
        }

        override fun bindNull(index: Int) {
            observeBindOrStep()
            delegate.bindNull(index)
        }

        override fun getBlob(index: Int): ByteArray = delegate.getBlob(index)
        override fun getDouble(index: Int): Double = delegate.getDouble(index)
        override fun getLong(index: Int): Long = delegate.getLong(index)
        override fun getText(index: Int): String = delegate.getText(index)
        override fun isNull(index: Int): Boolean = delegate.isNull(index)
        override fun getColumnCount(): Int = delegate.getColumnCount()
        override fun getColumnName(index: Int): String = delegate.getColumnName(index)
        override fun getColumnType(index: Int): Int = delegate.getColumnType(index)

        override fun step(): Boolean {
            observeBindOrStep()
            return delegate.step()
        }

        override fun reset() {
            events += "reset"
            resetCalls++
            if (injectCleanupFailure && mode == JvmCleanupFailureMode.RESET_THEN_CLEAR) {
                cleanupFailureObserved = true
                throw resetFailure
            }
            delegate.reset()
        }

        override fun clearBindings() {
            events += "clear"
            clearCalls++
            if (injectCleanupFailure && mode != JvmCleanupFailureMode.CLOSE_ONCE) {
                cleanupFailureObserved = true
                throw clearFailure
            }
            delegate.clearBindings()
        }

        override fun close() {
            events += "close"
            closeCalls++
            if (injectCleanupFailure && mode == JvmCleanupFailureMode.CLOSE_ONCE && closeCalls == 1) {
                cleanupFailureObserved = true
                throw closeFailure
            }
            delegate.close()
        }

        private fun observeBindOrStep() {
            if (!cleanupFailureObserved || !observePostCleanupWork) return
            allStatementBindOrStepCallsAfterFailure++
            if (injectCleanupFailure) {
                bindOrStepCallsAfterFailure++
            } else {
                differentStatementBindOrStepCallsAfterFailure++
            }
            throw postCleanupWorkFailure
        }
    }

    companion object {
        fun install(
            db: SafeSQLiteConnection,
            mode: JvmCleanupFailureMode,
            sqlMatcher: (String) -> Boolean,
        ): JvmStatementCleanupFault {
            val connectionField = SafeSQLiteConnection::class.java.getDeclaredField("ref")
            connectionField.isAccessible = true
            val originalConnection = connectionField.get(db) as SQLiteConnection
            return JvmStatementCleanupFault(mode, originalConnection, sqlMatcher).also { fault ->
                connectionField.set(db, fault.connection)
            }
        }
    }
}

private fun Throwable.countIdentity(
    target: Throwable,
    visited: MutableList<Throwable> = mutableListOf(),
): Int {
    if (visited.any { it === this }) return 0
    visited += this
    return (if (this === target) 1 else 0) +
        (cause?.countIdentity(target, visited) ?: 0) +
        suppressed.sumOf { it.countIdentity(target, visited) }
}
