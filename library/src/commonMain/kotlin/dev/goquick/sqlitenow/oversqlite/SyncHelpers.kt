package dev.goquick.sqlitenow.oversqlite

data class SyncRun(
    val upload: UploadSummary,
    val downloaded: Int,
)

/**
 * Convenience one-shot sync helper that performs an upload, then downloads
 * changes in pages until a non-full page is applied.
 *
 * When to prefer this
 * - Highly bi-directional, chatty flows (e.g., multiple devices frequently
 *   editing the same data). In these cases, pairing an upload with a bounded
 *   download helps converge local state with minimal ceremony.
 *
 * When to stay explicit
 * - Mostly upload-centric scenarios (one primary writer). Calling [uploadOnce]
 *   on its own reduces unnecessary round-trips. You can then schedule periodic
 *   or event-driven [downloadOnce] separately (e.g., on app resume).
 *
 * This helper preserves the recommended order (upload → download). It relies on
 * the client’s internal post-upload lookback to avoid resurrecting deletes.
 */
suspend fun OversqliteClient.syncOnce(
    limit: Int = 1000,
    includeSelf: Boolean = false
): Result<SyncRun> {
    // 1) Upload pending changes
    val up = uploadOnce()
    if (up.isFailure) return Result.failure(up.exceptionOrNull()!!)

    // 2) Download until applied < limit
    var total = 0
    var more = true
    while (more) {
        val dn = downloadOnce(limit = limit, includeSelf = includeSelf)
        if (dn.isFailure) return Result.failure(dn.exceptionOrNull()!!)
        val (applied, _) = dn.getOrNull() ?: (0 to 0L)
        total += applied
        more = applied == limit && limit > 0
        if (applied == 0) break
    }

    return Result.success(SyncRun(upload = up.getOrThrow(), downloaded = total))
}
