package dev.goquick.sqlitenow.oversqlite

// Inherit the exact public-call assertions and four backlog fixtures. Only the
// measured invocation is wrapped; JVM/SQLite setup is outside each sample.
internal class SettlementMemoryTest : SettlementTest() {
    override suspend fun measuredPush(client: DefaultOversqliteClient) {
        val sampler=JvmMemorySampler.start()
        try {super.measuredPush(client)}
        finally {println("A3 JVM measured public PushPending: ${sampler.stop()}")}
    }
}
