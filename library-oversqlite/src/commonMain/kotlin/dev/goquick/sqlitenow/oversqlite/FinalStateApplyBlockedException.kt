package dev.goquick.sqlitenow.oversqlite

/**
 * Correct the named application input, then retry ordinary synchronization. No checkpoint, pending
 * intent or business effect is committed on this error.
 */
class FinalStateApplyBlockedException(
    val reason: String,
    val operation: String,
    val table: String,
    val index: String = "",
    val trigger: String = "",
    val keys: List<String> = emptyList(),
    val hasMore: Boolean = false,
) : RuntimeException("final state apply blocked: $reason ($table)")
