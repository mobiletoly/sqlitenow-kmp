/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import java.io.OutputStream
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

internal class RequestGzipCancellationJvmTest : CrossTargetSyncTestSupport() {
    @Test
    fun midCompressionCancellationClosesStreamAndPreservesFrozenPush() = runBlocking {
        val firstChunkWritten = CountDownLatch(1)
        val releaseWriter = CountDownLatch(1)
        val firstWrite = AtomicBoolean(true)
        val replayAsIdentity = AtomicBoolean(false)
        val inputSize = AtomicInteger()
        val inputBytesWritten = AtomicInteger()
        val streamsCreated = AtomicInteger()
        val streamsClosed = AtomicInteger()
        val engine = OversqliteRequestGzipEngine { decoded, cap ->
            if (replayAsIdentity.get()) return@OversqliteRequestGzipEngine null
            inputSize.set(decoded.size)
            compressOversqliteRequestGzipWithStreamFactory(decoded, cap) { output: OutputStream ->
                streamsCreated.incrementAndGet()
                object : GZIPOutputStream(output, 8 * 1024) {
                    override fun write(bytes: ByteArray, offset: Int, length: Int) {
                        super.write(bytes, offset, length)
                        inputBytesWritten.addAndGet(length)
                        if (firstWrite.compareAndSet(true, false)) {
                            firstChunkWritten.countDown()
                            check(releaseWriter.await(5, TimeUnit.SECONDS)) {
                                "test did not release the active compressor"
                            }
                        }
                    }

                    override fun close() {
                        super.close()
                        streamsClosed.incrementAndGet()
                    }
                }
            }
        }
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, requestGzipEngine = engine)
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            insertUser(db, "user-1", "A".repeat(32_000))
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val nextSourceBundleId = scalarLong(
                db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'",
            )
            assertEquals(1L, nextSourceBundleId)
            assertEquals(outboxStateNone, scalarText(db, "SELECT state FROM _sync_outbox_bundle"))

            val syncing = async(Dispatchers.Default) { client.sync() }
            try {
                assertTrue(firstChunkWritten.await(5, TimeUnit.SECONDS), "gzip did not write its first chunk")
                assertEquals(1, streamsCreated.get())
                assertEquals(0, streamsClosed.get())
                assertTrue(syncing.isActive)
                syncing.cancel(CancellationException("caller cancelled during gzip write"))
            } finally {
                releaseWriter.countDown()
            }
            withTimeout(3_000) { syncing.join() }
            assertFailsWith<CancellationException> { syncing.await() }
            assertEquals(1, streamsClosed.get(), "the cancelled gzip stream must be closed")
            assertTrue(inputBytesWritten.get() < inputSize.get(), "cancelled gzip must stop before consuming all input")
            assertEquals(0, server.pushRequestCount)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(outboxStatePrepared, scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(sourceId, scalarText(db, "SELECT source_id FROM _sync_outbox_bundle"))
            assertEquals(nextSourceBundleId, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(
                nextSourceBundleId,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"),
            )

            // The mock server reads identity JSON; the cancellation assertions above use real gzip.
            replayAsIdentity.set(true)
            client.sync().getOrThrow()
            assertEquals(1, streamsCreated.get())
            assertEquals(1, streamsClosed.get())
            assertEquals(1, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(outboxStateNone, scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(
                nextSourceBundleId + 1L,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"),
            )
        } finally {
            releaseWriter.countDown()
            http.close()
            db.close()
        }
    }
}
