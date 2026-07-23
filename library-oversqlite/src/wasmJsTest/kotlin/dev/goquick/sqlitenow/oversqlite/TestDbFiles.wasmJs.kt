@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.JsFun
import kotlin.js.JsAny
import kotlin.js.Promise
import kotlin.random.Random
import kotlinx.coroutines.await

internal actual fun createTempSqliteNowTestDbPath(prefix: String): String =
    "$prefix-${Random.nextInt()}-${Random.nextInt()}.db"

private val testPersistence = IndexedDbSqlitePersistence(
    storageName = "SqliteNowOversqliteTest",
    storeName = "sqlite-tests",
)

internal actual fun createSqliteNowTestConnectionConfig(path: String): SqliteConnectionConfig =
    SqliteConnectionConfig(persistence = testPersistence)

internal actual suspend fun deleteTempSqliteNowTestDbArtifacts(path: String) {
    testPersistence.clear(path)
}

internal actual suspend fun cleanupDirectWorkerTestDatabase(dbName: String) {
    cleanupDirectWorkerTestDatabaseWasm(dbName).await<JsAny?>()
}

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
private external fun cleanupDirectWorkerTestDatabaseWasm(
    dbName: String,
): Promise<JsAny?>
