package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** The checkpoint advances to every applied bundle, so a pull page must never skip a sequence. */
class PullResponseContiguityTest {
    private fun bundle(seq: Long): Bundle {
        val row = BundleRow("main", "users", mapOf("id" to "$seq"), "INSERT", seq, Json.parseToJsonElement("""{"id":"$seq","name":"n"}"""))
        return Bundle(seq, "peer", seq, canonicalCommittedBundleHash(listOf(row)), listOf(row))
    }

    @Test
    fun contiguousPagesAreAccepted() {
        validatePullResponse(PullResponse(3, listOf(bundle(1), bundle(2), bundle(3)), false), afterBundleSeq = 0)
        validatePullResponse(PullResponse(5, listOf(bundle(3), bundle(4)), true), afterBundleSeq = 2)
        validatePullResponse(PullResponse(2, emptyList(), false), afterBundleSeq = 2)
    }

    @Test
    fun gapAfterFirstBundleIsRejected() {
        val error = assertFailsWith<IllegalArgumentException> {
            validatePullResponse(PullResponse(3, listOf(bundle(1), bundle(3)), false), afterBundleSeq = 0)
        }
        assertEquals("pull response bundle_seq 3 is not contiguous with previous 1", error.message)
    }

    @Test
    fun gapRightAfterCheckpointIsRejected() {
        val error = assertFailsWith<IllegalArgumentException> {
            validatePullResponse(PullResponse(3, listOf(bundle(2), bundle(3)), false), afterBundleSeq = 0)
        }
        assertEquals("pull response bundle_seq 2 is not contiguous with previous 0", error.message)
    }
}
