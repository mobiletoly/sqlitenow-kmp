package dev.goquick.sqlitenow.samplesynckmp

import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.SyncTable
import io.ktor.client.HttpClient
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class GeneratedOpeningOwnershipTest {
    @Test
    fun generatedOpeningRejectsMetadataMismatchThenReturnsExactReadyClient() = runTest {
        val database = newSampleSyncDatabase(":memory:")
        val http = HttpClient()
        val rejected = OversqliteConfig(
            generationId = 1L,
            databaseId = "sample-sync",
            serviceId = sampleSyncServerAppName,
            syncTables = listOf(SyncTable("wrong_table", syncKeyColumnName = "id")),
        )

        assertFailsWith<IllegalStateException> {
            database.openForSync(config = rejected, httpClient = http)
        }

        val config = database.buildOversqliteConfig(
            generationId = 1L,
            databaseId = "sample-sync",
            serviceId = sampleSyncServerAppName,
        )
        val opening = database.openForSync(config = config, httpClient = http)
        try {
            opening.advance()
            val ready = checkNotNull(opening.readyOrNull())
            val accessed = database.newOversqliteClient(
                generationId = 1L,
                databaseId = "sample-sync",
                serviceId = sampleSyncServerAppName,
                httpClient = http,
            )
            assertSame(ready.client, accessed)
        } finally {
            opening.close()
            http.close()
        }
    }
}
