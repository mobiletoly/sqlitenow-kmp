package dev.goquick.sqlitenow.core

import kotlin.test.*

class SqlTracePolicyTest {
    @Test fun redactsValuesCommentsAndQuotedIdentifiers() {
        val shape = sqlStatementShape("SELECT \"private_name\", X'CAFE', 123 FROM accounts --secret\nWHERE token='password''secret' /* grant */ AND id=:account", 4096, 512)!!
        listOf("private_name", "CAFE", "123", "accounts", "secret", "password", "token", "account").forEach { assertFalse(it in shape) }
        assertTrue(shape.startsWith("SELECT"))
        assertFalse('\n' in shape)
    }
    @Test fun malformedAndUnknownSyntaxIsOpaque() {
        listOf("SELECT 'secret", "SELECT /* secret", "SELECT [secret", "SELECT \\secret").forEach {
            assertNull(sqlStatementShape(it, 4096, 512))
        }
    }
    @Test fun inputAndOutputWorkAreBounded() {
        assertNull(sqlStatementShape("x".repeat(4097), 4096, 512))
        assertNull(sqlStatementShape("SELECT ".repeat(500), 4096, 64))
        assertNotNull(sqlStatementShape("SELECT 1", 8, 512))
    }
    @Test fun defaultsAreFiniteAndInvalidLimitsRejected() {
        val policy = SqlTracePolicy({})
        assertEquals(1000, policy.maxEvents); assertEquals(30000L, policy.durationMillis)
        assertFailsWith<IllegalArgumentException> { policy.copy(maxEvents = 0) }
        assertFailsWith<IllegalArgumentException> { policy.copy(maxInputChars = 4097) }
        assertFailsWith<IllegalArgumentException> { policy.copy(durationMillis = 0) }
    }
    @Test fun eventLimitStopsBeforeFormattingAndEmitsOneNotice() {
        val events = mutableListOf<String>(); var formatted = 0
        val trace = SqlTraceSession(SqlTracePolicy(events::add, maxEvents = 2), { 0 }) { _, _, _ -> formatted++; "SELECT ?" }
        repeat(100) { trace.record("prepare", "secret") }
        assertEquals(2, formatted); assertEquals(3, events.size)
        assertTrue(events.last().contains("truncated"))
    }
    @Test fun durationLimitStopsBeforeFormatting() {
        var now = 0L; var formatted = 0; val events = mutableListOf<String>()
        val trace = SqlTraceSession(SqlTracePolicy(events::add, durationMillis = 10), { now }) { _, _, _ -> formatted++; "SELECT ?" }
        trace.record("execute", "secret"); now = 10; repeat(4) { trace.record("execute", "secret") }
        assertEquals(1, formatted); assertEquals(2, events.size)
    }
    @Test fun sessionsHaveIndependentBudgets() {
        val a = mutableListOf<String>(); val b = mutableListOf<String>()
        val one = SqlTraceSession(SqlTracePolicy(a::add, maxEvents = 1), { 0 })
        val two = SqlTraceSession(SqlTracePolicy(b::add, maxEvents = 2), { 0 })
        repeat(5) { one.record("prepare", "SELECT 1"); two.record("prepare", "SELECT 2") }
        assertEquals(2, a.size); assertEquals(3, b.size)
    }
    @Test fun throwingFormatterDisablesSession() {
        var calls = 0; val events = mutableListOf<String>()
        val trace = SqlTraceSession(SqlTracePolicy(events::add), { 0 }) { _, _, _ -> calls++; error("formatter") }
        repeat(4) { trace.record("execute", "SELECT 1") }
        assertEquals(1, calls); assertTrue(events.isEmpty())
    }
    @Test fun throwingSinkDisablesSession() {
        var calls = 0
        val trace = SqlTraceSession(SqlTracePolicy({ calls++; error("sink") }), { 0 })
        repeat(4) { trace.record("execute", "SELECT 1") }
        assertEquals(1, calls)
    }
}
