package dev.goquick.sqlitenow.oversqlite

/**
 * Decoded size a multi-bundle pull page may have. A pull response is held in memory as a whole
 * while its bundles are applied, at several times its decoded size, so the page is sized by
 * bytes and not only by a bundle count. It equals the default snapshot chunk target.
 */
internal const val oversqlitePullPageTargetDecodedBytes: Long = 4L * 1024L * 1024L

/** Bundles asked for before the size of this account's bundles is known. */
internal const val oversqlitePullPageInitialBundles: Int = 16

/** Largest factor by which one page may exceed the previous request. */
private const val pullPageGrowthFactor: Long = 4

/**
 * Chooses how many bundles each request of one incremental pull asks for.
 *
 * The server pages by bundle count and by its own response limit, and a bundle can hold anything
 * from one row to the bundle byte limit. The first request is therefore small; each decoded page
 * then tells how many bundles of that size fill half the byte target, and the next request asks
 * for that many, growing by at most [pullPageGrowthFactor] per step. A page of several bundles
 * above the target is refused before its JSON is parsed and asked for again with fewer bundles.
 *
 * One bundle is indivisible. A request for a single bundle is limited only by what the server
 * advertised, so a bundle larger than the target is still pulled.
 */
internal class OversqlitePullPageSizing(
    private val maxBundles: Int,
    private val targetDecodedBytes: Long = oversqlitePullPageTargetDecodedBytes,
    initialBundles: Int = oversqlitePullPageInitialBundles,
) {
    init {
        require(maxBundles > 0) { "maxBundles must be positive" }
        require(targetDecodedBytes > 0) { "targetDecodedBytes must be positive" }
        require(initialBundles > 0) { "initialBundles must be positive" }
    }

    /** Bundles the next request asks for; always in `1..maxBundles`. */
    var requestBundles: Int = minOf(maxBundles, initialBundles)
        private set

    /** Decoded-body limit for the next request, given the limit the server advertised. */
    fun responseDecodedByteLimit(admittedDecodedBytes: Long): Long =
        if (requestBundles == 1) admittedDecodedBytes else minOf(admittedDecodedBytes, targetDecodedBytes)

    /** Records a decoded page and sizes the next request from it. */
    fun pageDecoded(bundles: Int, decodedBytes: Long) {
        if (bundles <= 0 || decodedBytes <= 0L) return
        val aimBytes = (targetDecodedBytes / 2L).coerceAtLeast(1L)
        val bytesPerBundle = (decodedBytes - 1L) / bundles + 1L // rounded up, never zero
        val fitting = aimBytes / bytesPerBundle
        val ceiling = minOf(maxBundles.toLong(), requestBundles * pullPageGrowthFactor)
        requestBundles = fitting.coerceIn(1L, ceiling).toInt()
    }

    /**
     * Shrinks the request after a page was refused for exceeding the byte target.
     * Returns false when the request is already a single bundle and cannot shrink.
     */
    fun shrinkAfterOversizedPage(): Boolean {
        if (requestBundles == 1) return false
        requestBundles = maxOf(1, requestBundles / pullPageGrowthFactor.toInt())
        return true
    }
}
