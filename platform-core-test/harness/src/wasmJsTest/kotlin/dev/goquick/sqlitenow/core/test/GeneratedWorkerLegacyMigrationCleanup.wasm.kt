@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core.test

import kotlin.js.JsAny
import kotlin.js.Promise
import kotlinx.coroutines.await

internal actual suspend fun cleanupGeneratedWorkerMigrationState(dbName: String) {
    cleanupGeneratedWorkerMigrationStateWasm(dbName).await<JsAny?>()
}

internal actual fun generatedWorkerLegacyMigrationBrowserAvailable(): Boolean =
    generatedWorkerLegacyMigrationBrowserAvailableWasm()

@JsFun("() => typeof window !== 'undefined'")
private external fun generatedWorkerLegacyMigrationBrowserAvailableWasm(): Boolean

@JsFun(
    """
    async (dbName) => {
      const bytes = new TextEncoder().encode(dbName);
      const digest = await crypto.subtle.digest("SHA-256", bytes);
      const hash = Array.from(new Uint8Array(digest))
        .map((byte) => byte.toString(16).padStart(2, "0")).join("");
      const target = "sqlitenow-worker-v1-" + hash + ".sqlite3";
      const root = await navigator.storage.getDirectory();
      for (const name of [target, target + ".health.json", target + ".migration.json"]) {
        try { await root.removeEntry(name); }
        catch (error) { if (error?.name !== "NotFoundError") throw error; }
      }
    }
    """,
)
private external fun cleanupGeneratedWorkerMigrationStateWasm(
    dbName: String,
): Promise<JsAny?>
