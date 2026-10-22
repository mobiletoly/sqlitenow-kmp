package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test

// Share the four backlog proofs without inheriting the ordinary trigger test.
// Only the public invocation is sampled; fixture setup remains outside it.
internal class SettlementMemoryTest : SettlementBacklogTestSupport() {
    @Test fun backlogBound() = runSettlementBacklog { client ->
        val sampler=JvmMemorySampler.start()
        try {client.pushPending().getOrThrow()}
        finally {println("A3 JVM measured public PushPending: ${sampler.stop()}")}
    }
}
