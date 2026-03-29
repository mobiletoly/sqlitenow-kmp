package dev.goquick.sqlitenow.oversqlite

import kotlin.test.assertEquals
import kotlin.test.assertIs

fun assertConnectedOutcome(
    expectedOutcome: AttachOutcome,
    actual: AttachResult,
    expectedAuthority: AuthorityStatus? = null,
) {
    val connected = assertIs<AttachResult.Connected>(actual)
    assertEquals(expectedOutcome, connected.outcome)
    expectedAuthority?.let { assertEquals(it, connected.status.authority) }
}
