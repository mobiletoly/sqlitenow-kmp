package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class OversqlitePullPageSizingTest {
    private val mib = 1024L * 1024L

    @Test
    fun firstRequestIsSmallAndHeldToTheByteTarget() {
        val pages = OversqlitePullPageSizing(maxBundles = 1000)
        assertEquals(oversqlitePullPageInitialBundles, pages.requestBundles)
        assertEquals(oversqlitePullPageTargetDecodedBytes, pages.responseDecodedByteLimit(32 * mib))
        assertEquals(2048L, pages.responseDecodedByteLimit(2048L), "a smaller advertised limit wins")

        assertEquals(5, OversqlitePullPageSizing(maxBundles = 5).requestBundles)
    }

    @Test
    fun smallBundlesGrowTheRequestByAtMostFourTimesPerPageUpToTheMaximum() {
        val pages = OversqlitePullPageSizing(maxBundles = 1000)
        val requested = mutableListOf(pages.requestBundles)
        repeat(4) {
            pages.pageDecoded(bundles = pages.requestBundles, decodedBytes = pages.requestBundles * 220L)
            requested += pages.requestBundles
        }
        assertEquals(listOf(16, 64, 256, 1000, 1000), requested)
    }

    @Test
    fun requestSettlesWhereBundlesFillHalfTheTarget() {
        val pages = OversqlitePullPageSizing(maxBundles = 1000, targetDecodedBytes = 4 * mib)
        val bundleBytes = 105_000L
        pages.pageDecoded(bundles = 16, decodedBytes = 16 * bundleBytes)
        assertEquals(19, pages.requestBundles) // 2 MiB / 105 kB
        pages.pageDecoded(bundles = 19, decodedBytes = 19 * bundleBytes)
        assertEquals(19, pages.requestBundles)
        assertTrue(pages.requestBundles * bundleBytes <= 2 * mib)

        // A short last page says nothing new about bundle size.
        pages.pageDecoded(bundles = 3, decodedBytes = 3 * bundleBytes)
        assertEquals(19, pages.requestBundles)
    }

    @Test
    fun oversizedPagesShrinkTheRequestUntilItIsOneBundle() {
        val pages = OversqlitePullPageSizing(maxBundles = 1000)
        val requested = mutableListOf(pages.requestBundles)
        while (pages.shrinkAfterOversizedPage()) requested += pages.requestBundles
        assertEquals(listOf(16, 4, 1), requested)
        assertFalse(pages.shrinkAfterOversizedPage())
        assertEquals(1, pages.requestBundles)
    }

    @Test
    fun oneBundleIsLimitedOnlyByWhatTheServerAdvertised() {
        val pages = OversqlitePullPageSizing(maxBundles = 1000)
        while (pages.shrinkAfterOversizedPage()) Unit
        assertEquals(32 * mib, pages.responseDecodedByteLimit(32 * mib))

        // A bundle above the target keeps the request at one bundle.
        pages.pageDecoded(bundles = 1, decodedBytes = 10 * mib)
        assertEquals(1, pages.requestBundles)
        assertEquals(32 * mib, pages.responseDecodedByteLimit(32 * mib))

        // Smaller bundles let it grow again, four times per page.
        pages.pageDecoded(bundles = 1, decodedBytes = 1000L)
        assertEquals(4, pages.requestBundles)
        assertEquals(oversqlitePullPageTargetDecodedBytes, pages.responseDecodedByteLimit(32 * mib))
    }

    @Test
    fun emptyPagesAndUnknownSizesLeaveTheRequestUnchanged() {
        val pages = OversqlitePullPageSizing(maxBundles = 1000)
        pages.pageDecoded(bundles = 0, decodedBytes = 60L)
        pages.pageDecoded(bundles = 3, decodedBytes = 0L)
        assertEquals(oversqlitePullPageInitialBundles, pages.requestBundles)
    }

    @Test
    fun requestStaysWithinItsBoundsForAnySequenceOfPages() {
        for (maxBundles in listOf(1, 2, 16, 17, 1000, 5000)) {
            val pages = OversqlitePullPageSizing(maxBundles = maxBundles, targetDecodedBytes = 4 * mib)
            var seed = 0x2545F491L + maxBundles
            repeat(2_000) {
                seed = seed * 6364136223846793005L + 1442695040888963407L
                val pick = (seed ushr 33).toInt()
                val before = pages.requestBundles
                if (pick % 5 == 0) {
                    val shrank = pages.shrinkAfterOversizedPage()
                    assertEquals(before > 1, shrank)
                    if (shrank) assertTrue(pages.requestBundles < before)
                } else {
                    val bundles = 1 + pick.mod(before)
                    val decodedBytes = listOf(1L, 300L, 105_000L, 3 * mib, 64 * mib, Long.MAX_VALUE)[pick.mod(6)]
                    pages.pageDecoded(bundles, decodedBytes)
                    assertTrue(pages.requestBundles <= before * 4L)
                }
                assertTrue(pages.requestBundles in 1..maxBundles, "request ${pages.requestBundles} for maximum $maxBundles")
            }
        }
    }
}
