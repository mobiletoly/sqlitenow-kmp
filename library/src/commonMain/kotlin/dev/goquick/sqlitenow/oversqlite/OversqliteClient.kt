package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.sqliteNetworkDispatcher
import kotlinx.coroutines.CoroutineDispatcher

/**
 * OversqliteClient exposes a minimal, durable API for the single‑user multi‑device sync flow.
 *
 * Responsibilities (client‑side):
 *  - Track local changes via SQLite triggers into a coalesced pending queue
 *  - Upload pending changes (with optimistic concurrency and idempotency)
 *  - Download ordered changes (excluding self by default) and apply atomically
 *  - Provide pause/resume controls for deterministic coordination with app code
 *
 * Best practices (recommended usage)
 * - Bootstrap
 *   - Call `bootstrap(userId, sourceId)` once after sign‑in (or DB creation) per device.
 *   - Ensure your business tables exist before bootstrap; the client installs triggers for the configured tables.
 *   - Keep foreign keys enabled (client does) and prefer simple UUID text primary keys named `id`.
 *
 * - Write locally, not through the client
 *   - Insert/update/delete rows directly in business tables; triggers coalesce changes into the pending queue.
 *   - Do not modify `_sync_*` tables yourself (implementation detail); let the client own them.
 *
 * - Sync loop (incremental)
 *   - After local edits (or on a cadence), call `uploadOnce()` then call `downloadOnce()` in a small loop.
 *   - Use a moderate `limit` (e.g., 200–1000) and repeat downloads while a full page is applied
 *     (see `downloadOnce` docs for the stopping condition).
 *   - Schedule this loop from app resume/foreground, or a background worker with sensible backoff and
 *     network constraints.
 *
 * - Recovery / first run
 *   - For large datasets or after reinstall, call `hydrate(windowed=true)` to download to a frozen snapshot
 *     in multiple pages with consistent results.
 *   - Prefer `includeSelf=false` (server already excludes your device id in normal incremental sync).
 *
 * - Pause/resume toggles
 *   - Use `pauseUploads()` around bulk imports or multi‑step UI edits to avoid draining partial work; resume when done.
 *   - Use `pauseDownloads()` to stabilize UX during critical screens (client already applies atomically, this is for app‑level coordination).
 *
 * - Conflicts
 *   - Provide a `Resolver` appropriate for your domain (e.g., server‑wins vs client‑wins with merge).
 *   - On conflict, the server returns the current row; resolve, update locally if needed, and the client will re‑enqueue as UPDATE.
 *
 * - Concurrency & performance
 *   - Use a single writable SQLite connection for the client (the client serializes writes internally).
 *   - Keep transactions short; avoid long‑running reads/writes in the same DB while sync is active.
 *   - Let the client enable WAL, foreign keys, and a modest busy_timeout; avoid your own conflicting PRAGMAs.
 */
interface OversqliteClient {
    /**
     * Suspend background uploads.
     *
     * When useful
     * - During bulk local writes (e.g., import/migration) to avoid draining incomplete rows
     *   into the pending queue mid‑operation.
     * - Around multi‑step edits in your UI where you prefer to enqueue once at the end.
     * - Before calling `hydrate` in custom flows (the SDK already suppresses triggers during
     *   download/hydrate internally via `apply_mode`, but pausing uploads can prevent other
     *   app code from enqueueing new changes while you coordinate state).
     *
     * Safe to call repeatedly.
     */
    suspend fun pauseUploads()

    /** Resume background uploads. Safe to call repeatedly. */
    suspend fun resumeUploads()

    /**
     * Suspend background downloads.
     *
     * When useful
     * - To keep the UI stable during critical on‑device workflows (e.g., checkout) so server
     *   updates don’t change the local forms mid‑flow.
     * - When you want to gate downloads to controlled windows (e.g., only on app resume or
     *   when on Wi‑Fi).
     *
     * Note: `downloadOnce` and `hydrate` perform atomic, trigger‑suppressed applies; pausing
     * downloads is for higher‑level UX coordination.
     *
     * Safe to call repeatedly.
     */
    suspend fun pauseDownloads()

    /** Resume background downloads. Safe to call repeatedly. */
    suspend fun resumeDownloads()

    /**
     * Initialize client metadata for the signed‑in user and device.
     *
     * When to call
     * - Call AFTER you have a stable, final user identity (e.g., after sign‑in when you have JWT `sub`).
     *   If the user changes (logout/login as someone else), open a clean DB (or clear metadata) and
     *   call `bootstrap` again with the new user.
     *
     * What `sourceId` is
     * - A stable, per‑install device identifier the server uses to tag changes and filter out
     *   "echo" on download (JWT `did` should equal this value).
     * - Generate once (UUIDv4), persist locally (SharedPreferences/Keychain), and reuse across app runs.
     * - After an uninstall/reinstall, generate a NEW `sourceId` (recommended). Reusing the old id can
     *   cause idempotency collisions unless you also migrate `next_change_id` to be higher than any
     *   previously uploaded `source_change_id` for that `(user, sourceId)` pair.
     *
     * Example workflow
     * ```kotlin
     * // 1) Sign in → obtain stable user id (e.g., from JWT `sub`)
     * val userId = authResult.userSub
     *
     * // 2) Ensure a stable per‑device source id
     * //    (below is Android example)
     * val prefs = appContext.getSharedPreferences("sync", 0)
     * val sourceId = prefs.getString("source_id", null) ?: UUID.randomUUID().toString().also {
     *   prefs.edit().putString("source_id", it).apply()
     * }
     *
     * // 3) Open DB + ensure business tables exist
     * //    (Users/posts tables etc.; then call bootstrap to install sync triggers)
     * client.bootstrap(userId = userId, sourceId = sourceId)
     *
     * // 4) Start your incremental sync loop (uploadOnce + downloadOnce)
     * ```
     */
    suspend fun bootstrap(userId: String, sourceId: String): Result<Unit>

    /**
     * Perform one upload cycle.
     * Upload pending changes once and return an [UploadSummary] with per-batch counts
     * (applied/conflict/invalid/materialize_error).
     */
    suspend fun uploadOnce(): Result<UploadSummary>

    /**
     * Perform one download cycle and apply a page of ordered changes locally.
     *
     * What it does
     * - Fetches up to `limit` changes from the per‑user ordered stream after the client’s
     *   current cursor.
     * - Applies the page atomically (one transaction), deferring FK checks and suppressing
     *   local triggers.
     * - Skips changes from the same device by default (`includeSelf=false`) to avoid echo.
     * - Advances the local cursor to the server‑provided `next_after` watermark.
     *
     * Typical usage
     * - Incremental sync loop (most apps): call with defaults (`includeSelf=false`, `until=0`)
     *   on a timer or after uploads. Keep calling while there might be more to apply.
     *
     *   How to know you should call again
     *   - If `appliedCount == limit`, the page was full — likely more remains. Call again.
     *   - If `appliedCount > 0 && appliedCount < limit`, you’re probably done for now.
     *   - If `appliedCount == 0`, there was nothing to apply. You may still see `nextAfter`
     *     advance (cursor bump without changes), which the client persists; you can stop.
     *
     *   Example
     *   ```kotlin
     *   var more = true
     *   while (more) {
     *     val (applied, _) = client.downloadOnce(limit = 500).getOrThrow()
     *     more = applied == 500
     *   }
     *   ```
     * - Recovery/rehydration: prefer `hydrate(windowed=true)` which wraps the advanced windowing
     *   behavior for you (see below). Only use `until` directly for advanced/manual flows.
     *
     * About the `until` parameter (advanced)
     * - The server’s stream is append‑only. When a dataset is large and requires multiple pages,
     *   new writes could appear between your first and second download calls.
     * - Passing a positive `until` tells the server to freeze an upper bound ("frozen window")
     *   so you see a consistent snapshot across pages: only changes with `server_id <= until`
     *   are returned.
     * - How to obtain `until` manually? Make an initial call with `until=0` to compute a window,
     *   then use the server’s `window_until` in subsequent calls. In this SDK, the higher‑level
     *   `hydrate(windowed=true)` handles this for you and is strongly recommended.
     *
     * Parameters
     * - limit: maximum number of changes to fetch (1..1000). Higher values reduce round‑trips
     *   but increase tx size.
     * - includeSelf: include changes from this same device. Use false for normal sync; true
     *   for recovery flows
     *   (e.g., after an app reinstall when you want to rebuild local state from the server).
     * - until: optional upper bound for advanced paging. Leave as 0 unless you know you need
     *   a frozen snapshot.
     *
     * Returns
     * - Result of Pair(appliedCount, nextAfter)
     *   - appliedCount: number of changes applied in this call
     *   - nextAfter: the new cursor watermark (you can persist/log it; the client also stores
     *     it internally)
     */
    suspend fun downloadOnce(
        limit: Int = 1000,
        includeSelf: Boolean = false,
        until: Long = 0L
    ): Result<Pair<Int, Long>>

    /** Release any resources (e.g., close database/HTTP clients) held by the client. */
    fun close()

    /**
     * Hydrate the local database by downloading the full user dataset in pages.
     *
     * Why use this
     * - Recovery or first‑run scenarios where you need to materialize all server state locally.
     * - Ensures referential integrity with deferred FK checks and suppresses local triggers
     *   during apply.
     *
     * Modes
     * - windowed=true (recommended): computes and uses a frozen upper bound (`window_until`) so
     *   the multi‑page hydration observes a consistent snapshot, even if new writes arrive during
     *   the process.
     * - windowed=false: simpler paging without a frozen bound; fine for small datasets where
     *   consistency across multiple pages is less critical.
     */
    suspend fun hydrate(
        includeSelf: Boolean = false,
        limit: Int = 1000,
        windowed: Boolean = true
    ): Result<Unit>
}

/**
 * PlatformDispatchers allows swapping dispatchers in tests or platform‑specific builds.
 * By default, IO‑like work uses the platform-aware sqliteNetworkDispatcher.
 */
open class PlatformDispatchers {
    open val io: CoroutineDispatcher = sqliteNetworkDispatcher()
}
